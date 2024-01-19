/*
FractalFlake is an implementation of the snowflakeID system used by twitter
It endevours create uncoordinated IDs across multiple nodes.

The ID is a 64bit integer.
0000000000000000000000000000000000000000000000000000000000000000
*---------------------------------------**---**----**----------*
Time since Fractal Epoch                NodeID ThreadID Sequence

Whenever a node joins the network it should communicate with a
coordinator node which will give it the FractalEpoch 
*/

// Read local file
// Write local file
// Read coordinator file
// Write coordinator file
// Create flake
// Read flake

use std::time::{SystemTime, UNIX_EPOCH};
use std::fs;
use thiserror::Error;

use isahc::prelude::*;
use serde::Deserialize;

/* ERRORS */
#[derive(Error, Debug)]
pub enum FractalError {
    #[error("IO Error while reading config")]
    IOError(#[from] std::io::Error),
    #[error("Missing equals sign at line {line:?}")]
    MissingEquals {
        line : usize
    },
    #[error("Invalid port value of {value:?} at line {line:?}")]
    InvalidPort {
        line : usize,
        value : String
    },
    #[error("Invalid node value of {value:?} at line {line:?}")]
    InvalidNode {
        line : usize,
        value : String
    },
    #[error("Invalid epoch value of {value:?} at line {line:?}")]
    InvalidEpoch {
        line : usize,
        value : String,
    },
    #[error("Network error while contacting {host:?}:{port:?}")]
    NetworkError {
        host : String,
        port : u16
    },
    #[error("Invalid json data from server")]
    DeserialisationError,
    #[error("Invalid sync epoch recived from server")]
    InvalidSyncEpochRecived,
    #[error("Error")]
    ErrorValue
}

#[derive(Deserialize)]
struct SyncResponse {
    epoch : String
}

pub struct FlakeSeed {
    sync_host : String,
    sync_port : u16,

    node_id : u64,
    epoch : u128
}

impl FlakeSeed {
    pub fn new(sync_host : String, sync_port : u16) -> FlakeSeed {
        FlakeSeed {
            sync_host : sync_host,
            sync_port : sync_port,
            node_id : 0,
            epoch : 0
        }
    }

    pub fn from_file(file_path : &str) -> Result<FlakeSeed, FractalError> {
        // Load file
        let contents = match fs::read_to_string(file_path) {
            Ok(c) => c,
            Err(e) => {return Err(FractalError::IOError(e));}
        };

        let mut seed = FlakeSeed::new("".to_string(), 0);

        // Read lines
        let mut f_ptr = 0;
        let mut line_number = 1;
        loop {
            if f_ptr >= contents.len() {
                break;
            }
            
            let line = Self::get_line(&contents, &mut f_ptr);
            
            // Split line
            let (lhs, rhs) = match Self::split_line(line) {
                Ok(r) => r,
                Err(_e) => {return Err(FractalError::MissingEquals { line: line_number });}
            };

            if lhs == "host" {
                seed.sync_host = rhs.to_string();
            }
            else if lhs == "port" {
                let port = match rhs.parse::<u16>() {
                    Ok(p) => p,
                    Err(_e) => {return Err(FractalError::InvalidPort {line:line_number, value:rhs.to_string()});}
                };
                seed.sync_port = port;
            }
            else if lhs == "node" {
                let node = match rhs.parse::<u64>() {
                    Ok(p) => p,
                    Err(_e) => {return Err(FractalError::InvalidNode {line:line_number, value:rhs.to_string()});}
                };
                seed.node_id = node;
            }
            else if lhs == "epoch" {
                let epoch = match rhs.parse::<u128>() {
                    Ok(p) => p,
                    Err(_e) => {return Err(FractalError::InvalidEpoch {line:line_number, value:rhs.to_string()});}
                };
                seed.epoch = epoch;
            }
            

            line_number += 1;
        };

        Ok(seed)
    }

    /*
    Syncs the seed with the given host
     */
    pub fn sync(&mut self) -> Result<(), FractalError> {
        let mut response = match isahc::get(format!("http://{}:{}/sync", self.sync_host, self.sync_port).as_str()) {
            Ok(r) => r,
            Err(_e) => {return Err(FractalError::NetworkError {host:self.sync_host.clone(), port:self.sync_port});}
        };

        let json = match response.json::<SyncResponse>() {
            Ok(j) => j,
            Err(_e) => {return Err(FractalError::DeserialisationError);}
        };

        self.epoch = match json.epoch.parse::<u128>()  {
            Ok(p) => p,
            Err(_e) => {return Err(FractalError::InvalidSyncEpochRecived);}
        };

        Ok(())
    }

    pub fn fracture(&self, thread_id : u64) -> FlakeGenerator {
        FlakeGenerator::new(self.epoch, self.node_id, thread_id)
    }

    fn get_line<'a>(buffer : &'a str, offset : &mut usize) -> &'a str {
        let mut count = 0;
        for c in buffer.chars().skip(*offset) {
            if c == '\n' {
                break;
            }
            count += 1;
        };
        
        let out = &buffer[*offset..*offset+count];
        *offset += count + 1;
        return out.trim();
    }

    fn split_line(buffer : &str) -> Result<(&str, &str), FractalError>{
        let mut count = 0;
        for c in buffer.chars() {
            if c == '=' {
                break;
            }
            count += 1;
        };

        if count >= buffer.len() {
            return Err(FractalError::ErrorValue);
        };

        Ok((&buffer[0..count], &buffer[count+1..]))
    }
}


pub struct FlakeGenerator {
    pub epoch : u128,
    pub node_id : u64,
    pub thread_id : u64,
    pub sequence : u64,
    pub last_time : u128
}

impl FlakeGenerator {
    pub fn new(epoch : u128, node_id : u64, thread_id : u64) -> FlakeGenerator {
        return FlakeGenerator {
            epoch: epoch,
            node_id: node_id,
            thread_id: thread_id,
            sequence: 0,
            last_time: 0
        };
    }

    pub fn generate(&mut self) -> u64 {
        self.check_sequence();

        // Get time since Flake epoch
        self.last_time = SystemTime::now().duration_since(UNIX_EPOCH).expect("Time somehow went backwards. Uhhhh?").as_millis().into();
        let mut flake: u64 = self.last_time as u64;
        flake <<= 22; // Shift so we only use the first 42 bits of the epoch

        // NodeID is a 5 bit identifier for the machine requesting the fractal
        // the node_id is anded with 31 (0b11111) to get only the first 5 bits
        // This is then shifted to the right by 19 bits to position it within the id
        // And finally this is ored with the flake to add it
        flake |= (self.node_id & 31) << 17;

        // ThreadID is the id of the thread creating the thread
        // the thread_id is anded with 31 (0b11111) to get only the first 5 bits
        // This is then shifted to the right by 13 bits to position it
        // Finally it is ored with the flake to add it
        flake |= (self.thread_id & 31) << 12;

        // Sequence is the id within the current millisecond
        // And with 4095 to get the bits
        // Or with flake to add
        flake |= self.sequence & 4095;

        self.sequence += 1;

        return flake;
    }

    fn check_sequence(&mut self) {
        // Check that sequence isn't maxed out
        if self.sequence > 4095 {
            while SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_millis() <= self.last_time {}
            self.sequence = 0;
            return;
        }

        // Check if time has elapsed
        if SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_millis() > self.last_time {
            self.sequence = 0;
        }
    }

}



#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn load_config() {
        let seed = FlakeSeed::from_file("D:/Programs/Rust/fractal_flake/test_config.cfg").unwrap();
        println!("Host: {}, Port: {}", seed.sync_host, seed.sync_port);
    }

    #[test]
    fn contact_server() {
        let mut seed = FlakeSeed::from_file("D:/Programs/Rust/fractal_flake/test_config.cfg").unwrap();
        seed.sync().unwrap();

        println!("Node ID: {}, Fractal Epoch: {}", seed.node_id, seed.epoch);
    }

    #[test]
    fn full() {
        let mut seed = FlakeSeed::from_file("D:/Programs/Rust/fractal_flake/test_config.cfg").unwrap();
        seed.sync().unwrap();

        let mut gen = seed.fracture(thread_id::get() as u64);
        let id = gen.generate();
        println!("Generated ID: {}", id);
    }

}
