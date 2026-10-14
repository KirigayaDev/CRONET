//! Разбор gNMI-путей вида `/interfaces/interface[name=eth0]/state/counters`.

use contracts::gnmi::{Path, PathElem};

#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum PathError {
    #[error("пустой путь")]
    Empty,
    #[error("пустой элемент пути")]
    EmptyElem,
    #[error("некорректный ключ в элементе {0:?}")]
    BadKey(String),
    #[error("путь слишком длинный")]
    TooLong,
}

const MAX_ELEMS: usize = 64;

/// Разделяет по `/`, не трогая `/` внутри квадратных скобок (значения ключей).
fn split_elems(s: &str) -> Result<Vec<&str>, PathError> {
    let mut out = Vec::new();
    let (mut depth, mut start) = (0i32, 0usize);
    for (i, c) in s.char_indices() {
        match c {
            '[' => depth += 1,
            ']' => depth -= 1,
            '/' if depth == 0 => {
                out.push(&s[start..i]);
                start = i + 1;
            }
            _ => {}
        }
        if depth < 0 {
            return Err(PathError::BadKey(s.to_owned()));
        }
    }
    if depth != 0 {
        return Err(PathError::BadKey(s.to_owned()));
    }
    out.push(&s[start..]);
    Ok(out)
}

fn parse_elem(raw: &str) -> Result<PathElem, PathError> {
    if raw.is_empty() {
        return Err(PathError::EmptyElem);
    }
    let name_end = raw.find('[').unwrap_or(raw.len());
    let name = &raw[..name_end];
    if name.is_empty() {
        return Err(PathError::EmptyElem);
    }
    let mut elem = PathElem { name: name.to_owned(), key: Default::default() };
    let mut rest = &raw[name_end..];
    while !rest.is_empty() {
        let inner = rest.strip_prefix('[').ok_or_else(|| PathError::BadKey(raw.to_owned()))?;
        let close = inner.find(']').ok_or_else(|| PathError::BadKey(raw.to_owned()))?;
        let (k, v) = inner[..close].split_once('=').ok_or_else(|| PathError::BadKey(raw.to_owned()))?;
        if k.is_empty() {
            return Err(PathError::BadKey(raw.to_owned()));
        }
        elem.key.insert(k.to_owned(), v.to_owned());
        rest = &inner[close + 1..];
    }
    Ok(elem)
}

pub fn parse_path(s: &str, origin: &str) -> Result<Path, PathError> {
    let s = s.trim();
    if s.is_empty() {
        return Err(PathError::Empty);
    }
    let trimmed = s.trim_start_matches('/').trim_end_matches('/');
    // "/" - корень: пустой список элементов
    let elems = if trimmed.is_empty() {
        Vec::new()
    } else {
        split_elems(trimmed)?.into_iter().map(parse_elem).collect::<Result<Vec<_>, _>>()?
    };
    if elems.len() > MAX_ELEMS {
        return Err(PathError::TooLong);
    }
    Ok(Path { origin: origin.to_owned(), elem: elems, ..Default::default() })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_plain_and_keyed() {
        let p = parse_path("/interfaces/interface[name=eth0]/state/counters", "openconfig").unwrap();
        assert_eq!(p.origin, "openconfig");
        let names: Vec<_> = p.elem.iter().map(|e| e.name.as_str()).collect();
        assert_eq!(names, ["interfaces", "interface", "state", "counters"]);
        assert_eq!(p.elem[1].key["name"], "eth0");
    }

    #[test]
    fn slash_inside_key_is_kept() {
        let p = parse_path("/interfaces/interface[name=ge-0/0/1]/state", "").unwrap();
        assert_eq!(p.elem.len(), 3);
        assert_eq!(p.elem[1].key["name"], "ge-0/0/1");
    }

    #[test]
    fn multiple_keys_and_root() {
        let p = parse_path("/a/b[x=1][y=2]", "").unwrap();
        assert_eq!(p.elem[1].key.len(), 2);
        assert!(parse_path("/", "").unwrap().elem.is_empty());
    }

    #[test]
    fn rejects_bad_paths() {
        for bad in ["", "   ", "/a//b", "/a/[x=1]", "/a/b[x]", "/a/b[=1]", "/a/b[x=1", "/a/b]x=1[", "/a/b[x=1]junk"] {
            assert!(parse_path(bad, "").is_err(), "{bad:?}");
        }
        let long = format!("/{}", vec!["a"; 65].join("/"));
        assert_eq!(parse_path(&long, ""), Err(PathError::TooLong));
    }
}
