use bytes::{Buf, BytesMut};
use std::io;
use tokio_util::codec::Decoder;

/// RFC6587 octet counting is recognized by a decimal length followed by SP.
/// LF framing removes only LF; CR and all other payload bytes are preserved.
pub struct SyslogCodec {
    max: usize,
}
impl SyslogCodec {
    pub fn new(max: usize) -> Self {
        Self { max }
    }
}
fn invalid() -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, "invalid or oversized syslog frame")
}

impl Decoder for SyslogCodec {
    type Item = Vec<u8>;
    type Error = io::Error;

    fn decode(&mut self, src: &mut BytesMut) -> io::Result<Option<Self::Item>> {
        if src.first().is_some_and(u8::is_ascii_digit) {
            let digits = src.iter().take_while(|b| b.is_ascii_digit()).count();
            if src.get(digits) == Some(&b' ') {
                if digits > 10 || src[0] == b'0' {
                    return Err(invalid());
                }
                let mut len = 0usize;
                for digit in &src[..digits] {
                    len = len
                        .checked_mul(10)
                        .and_then(|v| v.checked_add(usize::from(digit - b'0')))
                        .ok_or_else(invalid)?;
                }
                if len > self.max {
                    return Err(invalid());
                }
                let header = digits + 1;
                if src.len() < header + len {
                    return Ok(None);
                }
                src.advance(header);
                return Ok(Some(src.split_to(len).to_vec()));
            }
            if digits == src.len() && src.len() <= 10 && src.len() <= self.max {
                return Ok(None);
            }
        }
        if let Some(end) = src.iter().position(|b| *b == b'\n') {
            if end > self.max {
                return Err(invalid());
            }
            let payload = src.split_to(end).to_vec();
            src.advance(1);
            return Ok(Some(payload));
        }
        if src.len() > self.max {
            return Err(invalid());
        }
        Ok(None)
    }

    fn decode_eof(&mut self, src: &mut BytesMut) -> io::Result<Option<Self::Item>> {
        match self.decode(src)? {
            Some(frame) => Ok(Some(frame)),
            None if src.is_empty() => Ok(None),
            None => Err(io::Error::new(io::ErrorKind::UnexpectedEof, "incomplete syslog frame")),
        }
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)]
    use super::*;
    #[test]
    fn newline_multiple_binary_and_cr() {
        let mut codec = SyslogCodec::new(16);
        let mut buf = BytesMut::from(&b"<1>\xff\r\nnext\n"[..]);
        assert_eq!(codec.decode(&mut buf).unwrap().unwrap(), b"<1>\xff\r");
        assert_eq!(codec.decode(&mut buf).unwrap().unwrap(), b"next");
        assert!(buf.is_empty());
    }
    #[test]
    fn octets_fragmented_with_embedded_lf_and_following_frame() {
        let mut codec = SyslogCodec::new(16);
        let mut buf = BytesMut::from("5");
        assert!(codec.decode(&mut buf).unwrap().is_none());
        buf.extend_from_slice(b" a\nb\xffc4 next");
        assert_eq!(codec.decode(&mut buf).unwrap().unwrap(), b"a\nb\xffc");
        assert_eq!(codec.decode(&mut buf).unwrap().unwrap(), b"next");
        assert!(buf.is_empty());
    }
    #[test]
    fn oversize_and_bad_lengths_are_rejected_without_payload_in_errors() {
        for input in ["12345\n", "12345", "5 abcde", "99999999999 x", "0 ", "01 x"] {
            let error = SyslogCodec::new(4).decode(&mut BytesMut::from(input)).unwrap_err();
            assert_eq!(error.kind(), io::ErrorKind::InvalidData);
            assert!(!error.to_string().contains(input));
        }
    }
    #[test]
    fn exact_limits_empty_frames_and_incomplete_eof() {
        let mut codec = SyslogCodec::new(4);
        assert_eq!(codec.decode(&mut BytesMut::from("abcd\n")).unwrap().unwrap(), b"abcd");
        assert_eq!(codec.decode(&mut BytesMut::from("4 abcd")).unwrap().unwrap(), b"abcd");
        assert!(codec.decode(&mut BytesMut::from("\n")).unwrap().unwrap().is_empty());
        for input in ["partial", "4 abc", "12"] {
            assert!(codec.decode_eof(&mut BytesMut::from(input)).is_err());
        }
    }
}
