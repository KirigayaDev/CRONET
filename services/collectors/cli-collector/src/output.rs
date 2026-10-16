use encoding_rs::{CoderResult, Decoder, Encoding};

/// Streaming decoder and fixed-buffer ANSI parser. Raw bytes are stored separately.
pub struct Normalizer {
    decoder: Decoder,
    parser: vte::Parser,
    terminal: Terminal,
}
#[derive(Default)]
struct Terminal {
    text: String,
    line: Vec<char>,
    cursor: usize,
    limit: usize,
}
impl vte::Perform for Terminal {
    fn print(&mut self, c: char) {
        if self.cursor < self.line.len() {
            self.line[self.cursor] = c;
            self.cursor += 1;
        } else if self.text.len() + self.line.len() * 4 + 4 <= self.limit {
            self.line.push(c);
            self.cursor = self.line.len();
        }
    }
    fn execute(&mut self, byte: u8) {
        match byte {
            b'\r' => self.cursor = 0,
            b'\n' => {
                self.text.extend(self.line.drain(..));
                if self.text.len() < self.limit {
                    self.text.push('\n');
                }
                self.cursor = 0;
            }
            8 => self.cursor = self.cursor.saturating_sub(1),
            b'\t' => self.print('\t'),
            _ => {}
        }
    }
    fn csi_dispatch(&mut self, params: &vte::Params, _: &[u8], _: bool, action: char) {
        let value = params.iter().next().and_then(|p| p.first()).copied().unwrap_or(0) as usize;
        match action {
            'K' => match value {
                0 => self.line.truncate(self.cursor),
                2 => {
                    self.line.clear();
                    self.cursor = 0;
                }
                _ => {}
            },
            'G' => self.cursor = value.saturating_sub(1).min(self.line.len()),
            'D' => self.cursor = self.cursor.saturating_sub(value.max(1)),
            'C' => self.cursor = self.cursor.saturating_add(value.max(1)).min(self.line.len()),
            _ => {} // Styling/cursor movement outside current line is deliberately ignored.
        }
    }
}
impl Normalizer {
    pub fn new(encoding: &'static Encoding, raw_limit: usize) -> Self {
        Self {
            decoder: encoding.new_decoder_without_bom_handling(),
            parser: vte::Parser::new(),
            terminal: Terminal { limit: raw_limit.saturating_mul(4).saturating_add(16), ..Default::default() },
        }
    }
    pub fn feed(&mut self, mut data: &[u8], last: bool) {
        loop {
            let mut decoded = [0; 8192];
            let (status, read, written, _) = self.decoder.decode_to_utf8(data, &mut decoded, last);
            self.parser.advance(&mut self.terminal, &decoded[..written]);
            data = &data[read..];
            if status == CoderResult::InputEmpty {
                break;
            }
        }
    }
    /// Only the final 4096 characters are considered for prompt detection.
    pub fn tail(&self) -> String {
        let mut tail: Vec<char> = self.terminal.line.iter().rev().copied().take(4096).collect();
        if tail.len() < 4096 {
            tail.extend(self.terminal.text.chars().rev().take(4096 - tail.len()));
        }
        tail.reverse();
        tail.into_iter().collect()
    }
    pub fn finish(mut self) -> String {
        self.feed(&[], true);
        self.take_text()
    }
    /// Keep decoder/ANSI state across command boundaries on the same terminal.
    pub fn take_text(&mut self) -> String {
        self.terminal.text.extend(self.terminal.line.drain(..));
        self.terminal.cursor = 0;
        std::mem::take(&mut self.terminal.text)
    }
}
pub fn normalize(data: &[u8], encoding: &'static Encoding) -> String {
    let mut n = Normalizer::new(encoding, data.len());
    n.feed(data, false);
    n.finish()
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn fragmented_utf8_ansi_osc_and_terminal_controls() {
        let raw = "\x1b[32mПривет\x1b[0m\r\nabc\rX\x1b[K\r\nrouter# \x1b]0;title\x07".as_bytes();
        let mut n = Normalizer::new(encoding_rs::UTF_8, raw.len());
        for b in raw {
            n.feed(&[*b], false);
        }
        assert!(n.tail().ends_with("router# "));
        assert_eq!(n.finish(), "Привет\nX\nrouter# ");
    }
    #[test]
    fn legacy_and_invalid_utf8_preserve_decodable_text() {
        assert_eq!(normalize(&[0xcf, 0xf0, 0xe8, 0xe2, 0xe5, 0xf2], encoding_rs::WINDOWS_1251), "Привет");
        assert_eq!(normalize(&[255], encoding_rs::UTF_8), "�");
    }
    #[test]
    fn unterminated_osc_is_bounded() {
        let mut n = Normalizer::new(encoding_rs::UTF_8, 1024);
        n.feed(b"\x1b]0;", false);
        for _ in 0..1000 {
            n.feed(&[b'x'; 1024], false);
        }
        n.feed(b"\x07router#", false);
        assert_eq!(n.finish(), "router#");
    }
    #[test]
    fn ansi_state_survives_prompt_at_command_boundary() {
        let mut n = Normalizer::new(encoding_rs::UTF_8, 1024);
        n.feed(b"router# \x1b[", false);
        assert_eq!(n.take_text(), "router# ");
        n.feed(b"0mnext command\r\nrouter# ", false);
        assert_eq!(n.finish(), "next command\nrouter# ");
    }
}
