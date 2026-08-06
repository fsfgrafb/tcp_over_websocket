use anyhow::{Result, anyhow, bail};

pub const PROTOCOL_VERSION: u16 = 2;
pub const HEADER_LEN: usize = 5;
pub const MAX_PAYLOAD_LEN: usize = u16::MAX as usize;
pub const MAX_DATA_LEN: usize = u16::MAX as usize;
pub const MAX_OPEN_LEN: usize = 255;
pub const MAX_ERROR_LEN: usize = 256;
pub const MAX_VERSION_PAYLOAD_LEN: usize = 128;
pub const MAX_TUNNELS: usize = 64;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum FrameType {
    Hello = 0x00,
    Open = 0x01,
    Data = 0x02,
    Close = 0x03,
    Ping = 0x04,
    OpenOk = 0x05,
    OpenFail = 0x06,
    HelloAck = 0x07,
    Eof = 0x08,
}

impl TryFrom<u8> for FrameType {
    type Error = anyhow::Error;

    fn try_from(value: u8) -> Result<Self> {
        match value {
            0x00 => Ok(Self::Hello),
            0x01 => Ok(Self::Open),
            0x02 => Ok(Self::Data),
            0x03 => Ok(Self::Close),
            0x04 => Ok(Self::Ping),
            0x05 => Ok(Self::OpenOk),
            0x06 => Ok(Self::OpenFail),
            0x07 => Ok(Self::HelloAck),
            0x08 => Ok(Self::Eof),
            other => bail!("未知帧类型 0x{other:02x}"),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Frame {
    pub kind: FrameType,
    pub tunnel_id: u16,
    pub payload: Vec<u8>,
}

impl Frame {
    pub fn new(kind: FrameType, tunnel_id: u16, payload: Vec<u8>) -> Result<Self> {
        if payload.len() > MAX_PAYLOAD_LEN {
            bail!("帧负载超过 65,535 字节");
        }
        let frame = Self {
            kind,
            tunnel_id,
            payload,
        };
        frame.validate_shape()?;
        Ok(frame)
    }

    pub fn hello(program: &str) -> Result<Self> {
        Self::version_frame(FrameType::Hello, program)
    }

    pub fn hello_ack(program: &str) -> Result<Self> {
        Self::version_frame(FrameType::HelloAck, program)
    }

    fn version_frame(kind: FrameType, program: &str) -> Result<Self> {
        if program.is_empty() {
            bail!("程序版本字符串不能为空");
        }
        let mut payload = Vec::with_capacity(2 + program.len());
        payload.extend_from_slice(&PROTOCOL_VERSION.to_be_bytes());
        payload.extend_from_slice(program.as_bytes());
        Self::new(kind, 0, payload)
    }

    pub fn encode(&self) -> Vec<u8> {
        let mut encoded = Vec::with_capacity(HEADER_LEN + self.payload.len());
        encoded.push(self.kind as u8);
        encoded.extend_from_slice(&self.tunnel_id.to_be_bytes());
        encoded.extend_from_slice(&(self.payload.len() as u16).to_be_bytes());
        encoded.extend_from_slice(&self.payload);
        encoded
    }

    pub fn decode(encoded: &[u8]) -> Result<Self> {
        if encoded.len() < HEADER_LEN {
            bail!("帧长度小于 {HEADER_LEN} 字节");
        }
        let kind = FrameType::try_from(encoded[0])?;
        let tunnel_id = u16::from_be_bytes([encoded[1], encoded[2]]);
        let payload_len = u16::from_be_bytes([encoded[3], encoded[4]]) as usize;
        if encoded.len() != HEADER_LEN + payload_len {
            bail!(
                "帧长度不匹配：声明 {payload_len} 字节，实际 {} 字节",
                encoded.len() - HEADER_LEN
            );
        }
        Self::new(kind, tunnel_id, encoded[HEADER_LEN..].to_vec())
    }

    pub fn version(&self) -> Result<(u16, &str)> {
        if !matches!(self.kind, FrameType::Hello | FrameType::HelloAck) {
            bail!("当前帧不是版本协商帧");
        }
        let version = u16::from_be_bytes([self.payload[0], self.payload[1]]);
        let program = std::str::from_utf8(&self.payload[2..]).context_utf8()?;
        Ok((version, program))
    }

    pub fn validate_client_to_server(&self, handshake_done: bool) -> Result<()> {
        if !handshake_done && self.kind != FrameType::Hello {
            bail!("握手前客户端只能发送 HELLO");
        }
        if handshake_done
            && matches!(
                self.kind,
                FrameType::Hello | FrameType::HelloAck | FrameType::OpenOk | FrameType::OpenFail
            )
        {
            bail!("客户端发送了方向错误或重复的 {:?} 帧", self.kind);
        }
        Ok(())
    }

    pub fn validate_server_to_client(&self, handshake_done: bool) -> Result<()> {
        if !handshake_done && self.kind != FrameType::HelloAck {
            bail!("握手前服务端只能发送 HELLO_ACK");
        }
        if handshake_done
            && matches!(
                self.kind,
                FrameType::Hello | FrameType::HelloAck | FrameType::Open | FrameType::Ping
            )
        {
            bail!("服务端发送了方向错误或重复的 {:?} 帧", self.kind);
        }
        Ok(())
    }

    fn validate_shape(&self) -> Result<()> {
        let connection_level = matches!(
            self.kind,
            FrameType::Hello | FrameType::HelloAck | FrameType::Ping
        );
        if connection_level && self.tunnel_id != 0 {
            bail!("连接级帧的 tunnel_id 必须为 0");
        }
        if !connection_level && (self.tunnel_id == 0 || self.tunnel_id == u16::MAX) {
            bail!("流级帧的 tunnel_id 必须在 1..=65534 范围内");
        }

        match self.kind {
            FrameType::Hello | FrameType::HelloAck => {
                if self.payload.len() < 3 || self.payload.len() > MAX_VERSION_PAYLOAD_LEN {
                    bail!("版本协商负载必须包含版本号和非空程序版本，且不超过 128 字节");
                }
                std::str::from_utf8(&self.payload[2..])
                    .map_err(|_| anyhow!("程序版本字符串不是合法 UTF-8"))?;
            }
            FrameType::Open => {
                if self.payload.is_empty() || self.payload.len() > MAX_OPEN_LEN {
                    bail!("OPEN 目标长度必须为 1..=255 字节");
                }
                std::str::from_utf8(&self.payload)
                    .map_err(|_| anyhow!("OPEN 目标不是合法 UTF-8"))?;
            }
            FrameType::OpenFail => {
                if self.payload.len() > MAX_ERROR_LEN {
                    bail!("OPEN_FAIL 错误文本不能超过 256 字节");
                }
                std::str::from_utf8(&self.payload)
                    .map_err(|_| anyhow!("OPEN_FAIL 错误文本不是合法 UTF-8"))?;
            }
            FrameType::Close | FrameType::Ping | FrameType::OpenOk | FrameType::Eof => {
                if !self.payload.is_empty() {
                    bail!("{:?} 帧的负载必须为空", self.kind);
                }
            }
            FrameType::Data => {}
        }
        Ok(())
    }
}

trait Utf8Context<'a> {
    fn context_utf8(self) -> Result<&'a str>;
}

impl<'a> Utf8Context<'a> for std::result::Result<&'a str, std::str::Utf8Error> {
    fn context_utf8(self) -> Result<&'a str> {
        self.map_err(|_| anyhow!("程序版本字符串不是合法 UTF-8"))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn frame_round_trip_including_maximum_data() {
        let frame = Frame::new(FrameType::Data, 7, vec![0x5a; MAX_DATA_LEN]).unwrap();
        let encoded = frame.encode();
        assert_eq!(Frame::decode(&encoded).unwrap(), frame);
    }

    #[test]
    fn rejects_wrong_length_unknown_type_and_direction() {
        assert!(Frame::decode(&[0x02, 0, 1, 0, 2, 1]).is_err());
        assert!(Frame::decode(&[0xff, 0, 0, 0, 0]).is_err());
        let frame = Frame::new(FrameType::OpenOk, 1, Vec::new()).unwrap();
        assert!(frame.validate_client_to_server(true).is_err());
    }

    #[test]
    fn hello_exposes_protocol_and_program_versions() {
        let hello = Frame::hello("towc 0.5.1").unwrap();
        assert_eq!(hello.version().unwrap(), (2, "towc 0.5.1"));
    }
}
