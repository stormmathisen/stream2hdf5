use filedescriptor::*;


use std::fs::File;
use std::fs::OpenOptions;

use std::io::prelude::*;
use std::sync::{
    atomic::{AtomicBool, Ordering},
    mpsc::{sync_channel, Receiver, TrySendError, TryRecvError},
};
use std::time::Duration;
use std::{thread, time};
use std::io::SeekFrom;
use std::ptr::write;

use anyhow::{Context, Result};
use chrono::prelude::*;
use byteorder::{LittleEndian, ReadBytesExt, WriteBytesExt};

//Timing
const HEARTBEAT_SLEEP_DURATION: Duration = Duration::from_micros(2500);

//File definitions
const BAR1_NAME: &str = "/home/storm/Desktop/hdf5rustlocal/pcie_bar1_s5";
const DMA_NAME: &str = "/home/storm/Desktop/hdf5rustlocal/pcie_dma_s5";
const NAS_LOC: &str = "/home/storm/Desktop/hdf5rustlocal/NAS/"; //Location to move hdf5 files at midnight
const TMP_LOC: &str = "/home/storm/Desktop/hdf5rustlocal/TMP/"; //Location to store locally

//Data definitions
const SAMPLES: usize = 512; //Number of samples in each array
static DONE: AtomicBool = AtomicBool::new(false);
const ADC_OFFSET: u64 = 160; //Offset to first ADC array
const ADC_LENGTH: u64 = 128; //Offset between ADC arrays
const ADC_NUM: u64= 10; //Number of ADCs
const ACTIVE_PULSE_OFFSET: u64 = 70; //Offset for active pulse registry
const TOTAL_PULSE_OFFSET: u64 = 71; //Offset for total pulse registry
const STATE_OFFSET: u64 = 66; //Offset for stat


#[derive(Debug)]
struct DataContainer {
    internal_count: u64,
    datetime: DateTime<Utc>,
    active_pulse: u64,
    total_pulse: u64,
    state: u32,
    kly_fwd_pwr: [u16; SAMPLES],
    kly_fwd_pha: [u16; SAMPLES],
    kly_rev_pwr: [u16; SAMPLES],
    kly_rev_pha: [u16; SAMPLES],
    cav_fwd_pwr: [u16; SAMPLES],
    cav_fwd_pha: [u16; SAMPLES],
    cav_rev_pwr: [u16; SAMPLES],
    cav_rev_pha: [u16; SAMPLES],
    cav_probe_pwr: [u16; SAMPLES],
    cav_probe_pha: [u16; SAMPLES]
}

impl IntoIterator for DataContainer {
    type Item = [u16; SAMPLES];
    type IntoIter = std::array::IntoIter<Self::Item, 10>;

    fn into_iter(self) -> Self::IntoIter {
        let iter_array = [
            self.kly_fwd_pwr,
            self.kly_fwd_pha,
            self.kly_rev_pwr,
            self.kly_rev_pha,
            self.cav_fwd_pwr,
            self.cav_fwd_pha,
            self.cav_rev_pwr,
            self.cav_rev_pha,
            self.cav_probe_pwr,
            self.cav_probe_pha
        ];
        iter_array.into_iter()
    }
}



fn main() -> Result<()> {
    //Handle ctrl+c by telling threads to finish
    ctrlc::set_handler(|| DONE.store(true, Ordering::SeqCst))?;

    //Initalize counters, files and channels
    let mut main_loop_counter: u64 = 0;

    let (datasender, datareceiver) = sync_channel::<DataContainer>(4);
    let (heartbeatsender, heartbeatreceiver) = sync_channel::<bool>(1);

    let mut dma_file = File::open(DMA_NAME)?;
    let mut bar_file = File::open(DMA_NAME)
        .with_context(|| format!("Failed to open {}", DMA_NAME))?;
    let mut bar_fd = FileDescriptor::dup(&bar_file)?;
    /*let mut poll_array = [
        pollfd {
            fd: bar_fd.into_raw_file_descriptor(),
            events: POLLIN,
            revents: 0
        }
    ];*/



    //Spawn fake heartbeat thread
    let heartbeat_handle = thread::spawn(move || {
        false_heartbeat(HEARTBEAT_SLEEP_DURATION, heartbeatreceiver).unwrap();
    }
    );

    //Spawn write thread
    let write_handle = thread::spawn(move ||{
        write_thread(datareceiver);
    }
    );

    //Main loop
    while !DONE.load(Ordering::Relaxed) {
        //Wait for file ready
        //poll(&mut poll_array, Some(Duration::from_millis(1))).context("Failed on polling BAR")?;
        let shot_start = time::Instant::now();
        let shot_timestamp = Utc::now();

        //Read data
        let data_container = DataContainer{
            internal_count: main_loop_counter,
            datetime: shot_timestamp,
            active_pulse: read_bar(&mut bar_file, ACTIVE_PULSE_OFFSET).context("Failed to read Active Pulse")?,
            total_pulse: read_bar(&mut bar_file, TOTAL_PULSE_OFFSET).context("Failed to read Total Pulse")?,
            state: read_bar(&mut bar_file, STATE_OFFSET).context("Failed to read State")? as u32,
            kly_fwd_pwr: read_dma(&mut dma_file, ADC_OFFSET + 0 * ADC_LENGTH).context("Failed to read kly_fwd_pwr")?,
            kly_fwd_pha: read_dma(&mut dma_file, ADC_OFFSET + 1 * ADC_LENGTH).context("Failed to read kly_fwd_pwr")?,
            kly_rev_pwr: read_dma(&mut dma_file, ADC_OFFSET + 2 * ADC_LENGTH).context("Failed to read kly_fwd_pwr")?,
            kly_rev_pha: read_dma(&mut dma_file, ADC_OFFSET + 3 * ADC_LENGTH).context("Failed to read kly_fwd_pwr")?,
            cav_fwd_pwr: read_dma(&mut dma_file, ADC_OFFSET + 4 * ADC_LENGTH).context("Failed to read kly_fwd_pwr")?,
            cav_fwd_pha: read_dma(&mut dma_file, ADC_OFFSET + 5 * ADC_LENGTH).context("Failed to read kly_fwd_pwr")?,
            cav_rev_pwr: read_dma(&mut dma_file, ADC_OFFSET + 6 * ADC_LENGTH).context("Failed to read kly_fwd_pwr")?,
            cav_rev_pha: read_dma(&mut dma_file, ADC_OFFSET + 7 * ADC_LENGTH).context("Failed to read kly_fwd_pwr")?,
            cav_probe_pwr: read_dma(&mut dma_file, ADC_OFFSET + 8 * ADC_LENGTH).context("Failed to read kly_fwd_pwr")?,
            cav_probe_pha: read_dma(&mut dma_file, ADC_OFFSET + 9 * ADC_LENGTH).context("Failed to read kly_fwd_pwr")?
        };

        let total_pulse = data_container.total_pulse;

        //Try sending data to channel

        match datasender.try_send(data_container) {
            Ok(()) => {} // cool
            Err(TrySendError::Full(_)) => {
                println!("DANGER WILL ROBINSON - writer not keeping up!")
            }
            Err(TrySendError::Disconnected(_)) => {
                // The receiving side hung up!
                // Bounce out of the loop to see what error it had.
                break;
            }
        }
        //Wait for next pulse (there must be a better way!)
        while read_bar(&mut bar_file, TOTAL_PULSE_OFFSET).context("Failed to read Total Pulse")? == total_pulse {
            std::hint::spin_loop();
        }

        println!{"Pulse number: {}. Time around the loop: {} us", total_pulse, shot_start.elapsed().as_micros()};

    }

    //Handle closing
    match heartbeatsender.try_send(true) {
        Ok(()) => {
            println!("Shutting down heartbeat thread");
            heartbeat_handle.join().unwrap();
        }
        Err(TrySendError::Full(_)) => {
            println!("Shutting down heartbeat thread");
            heartbeat_handle.join().unwrap();
        }
        Err(TrySendError::Disconnected(_)) => {}
    }
    //Join write thread to wait for shutdown
    println!("SHUTDOWN");
    Ok(())
}

fn read_bar(buffer: &mut File, offset: u64) ->Result<u64> {
    let mut output: [u64; 1] = [0; 1];
    buffer.seek(SeekFrom::Start(offset)).with_context(|| format!("Error while seeking to {} in {:?}", offset, buffer))?;
    buffer.read_u64_into::<LittleEndian>(&mut output).with_context(|| format!("Error while reading {:?} into {:?}", buffer, output))?;
    Ok(output[0])
}

fn read_dma(buffer: &mut File, offset: u64) -> Result<[u16; SAMPLES]> {
    let mut output: [u16; SAMPLES] = [0; SAMPLES];
    buffer.seek(SeekFrom::Start(offset)).with_context(|| format!("Error while seeking to {} in {:?}", offset, buffer))?;
    buffer.read_u16_into::<LittleEndian>(&mut output).with_context(|| format!("Error while reading {:?} into {:?}", buffer, output))?;
    Ok(output)
}


fn write_thread (receiver: Receiver<DataContainer>) {
    let mut write_start = time::Instant::now();
    while let Ok(received_data) = receiver.recv() {

        println!("Fin, took {} us", write_start.elapsed().as_micros());
        write_start = time::Instant::now();
    }

    }

/*
fn write_hdf() {

}

fn write_hdf_attr() {

}

fn write_hdf_ds() {

}
*/

fn false_heartbeat(pulse_rate: Duration, ctrl: Receiver<bool>) -> Result<()>{
    let mut pulse_counter: u64 = 0;

    let mut pfile = OpenOptions::new()
        .write(true)
        .open(DMA_NAME).with_context(|| format!("Fake heartbeat could not open {} in write mode", DMA_NAME))?;

    loop {
        let end_at = time::Instant::now() + pulse_rate;
        pfile.seek(SeekFrom::Start(TOTAL_PULSE_OFFSET)).context("Error while seeking in fake heartbeat")?;
        pfile.write_u64::<LittleEndian>(pulse_counter).with_context(|| format!("Failed to write {} to {}", pulse_counter, DMA_NAME))?;
        pulse_counter = pulse_counter.wrapping_add(1);

        while time::Instant::now() < end_at {
            std::hint::spin_loop();
        }
        println!{"Finished a heartbeat!"}

        match ctrl.try_recv() {
            Ok(control) => {
                //Main thread has commanded shutdown
                break;
            }
            Err(TryRecvError::Empty) => {}
            Err(TryRecvError::Disconnected) => {
                //Main thread has disconnected, let's go ask what's happening
                break;
            }
        }
    }
    Ok(())
}
