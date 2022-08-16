//use filedescriptor::*;


use std::fs::File;
use std::fs::OpenOptions;

use std::io::prelude::*;
use std::sync::{
    atomic::{AtomicBool, Ordering},
    mpsc::{sync_channel, Receiver, TrySendError, TryRecvError, RecvTimeoutError},
};
use std::time::Duration;
use std::{thread, time};
use std::io::SeekFrom;


use anyhow::{Context, Result};
use chrono::prelude::*;
use byteorder::{LittleEndian, ReadBytesExt, WriteBytesExt};

use hdf5;

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
const DATA_FIELD_NAMES: [&str; ADC_NUM as usize] = [
    "kly_fwd_pwr",
    "kly_fwd_pha",
    "kly_rev_pwr",
    "kly_rev_pha",
    "cav_fwd_pwr",
    "cav_fwd_pha",
    "cav_rev_pwr",
    "cav_rev_pha",
    "cav_probe_pwr",
    "cav_probe_pha"
];

//HDF5 definitions
const CHUNK_SIZE: usize = 256; //HDF5 chunk size



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
    println!("hdf5 threadsafe = {}", hdf5::is_library_threadsafe());

    //Initalize counters, files and channels
    let mut main_loop_counter: u64 = 0;

    let (datasender, datareceiver) = sync_channel::<DataContainer>(100);
    let (heartbeatsender, heartbeatreceiver) = sync_channel::<bool>(1);

    let mut dma_file = File::open(DMA_NAME)?;
    let mut bar_file = File::open(BAR1_NAME)
        .with_context(|| format!("Failed to open {}", DMA_NAME))?;
    /*let mut bar_fd = FileDescriptor::dup(&bar_file)?;
    let mut poll_array = [
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
        write_thread(datareceiver).context("Write thread error");
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
            kly_fwd_pwr: read_dma(&mut dma_file, ADC_OFFSET + 0 * ADC_LENGTH).with_context(|| format!("Failed to read {}", DATA_FIELD_NAMES[0]))?,
            kly_fwd_pha: read_dma(&mut dma_file, ADC_OFFSET + 1 * ADC_LENGTH).with_context(|| format!("Failed to read {}", DATA_FIELD_NAMES[1]))?,
            kly_rev_pwr: read_dma(&mut dma_file, ADC_OFFSET + 2 * ADC_LENGTH).with_context(|| format!("Failed to read {}", DATA_FIELD_NAMES[2]))?,
            kly_rev_pha: read_dma(&mut dma_file, ADC_OFFSET + 3 * ADC_LENGTH).with_context(|| format!("Failed to read {}", DATA_FIELD_NAMES[3]))?,
            cav_fwd_pwr: read_dma(&mut dma_file, ADC_OFFSET + 4 * ADC_LENGTH).with_context(|| format!("Failed to read {}", DATA_FIELD_NAMES[4]))?,
            cav_fwd_pha: read_dma(&mut dma_file, ADC_OFFSET + 5 * ADC_LENGTH).with_context(|| format!("Failed to read {}", DATA_FIELD_NAMES[5]))?,
            cav_rev_pwr: read_dma(&mut dma_file, ADC_OFFSET + 6 * ADC_LENGTH).with_context(|| format!("Failed to read {}", DATA_FIELD_NAMES[6]))?,
            cav_rev_pha: read_dma(&mut dma_file, ADC_OFFSET + 7 * ADC_LENGTH).with_context(|| format!("Failed to read {}", DATA_FIELD_NAMES[7]))?,
            cav_probe_pwr: read_dma(&mut dma_file, ADC_OFFSET + 8 * ADC_LENGTH).with_context(|| format!("Failed to read {}", DATA_FIELD_NAMES[8]))?,
            cav_probe_pha: read_dma(&mut dma_file, ADC_OFFSET + 9 * ADC_LENGTH).with_context(|| format!("Failed to read {}", DATA_FIELD_NAMES[9]))?
        };

        let total_pulse = data_container.total_pulse;

        //Try sending data to channel

        match datasender.try_send(data_container) {
            Ok(()) => {} // cool
            Err(TrySendError::Full(_)) => {
                println!("DANGER WILL ROBINSON - writer not keeping up!");
                break;
            }
            Err(TrySendError::Disconnected(_)) => {
                // The receiving side hung up!
                // Bounce out of the loop to see what error it had.
                break;
            }
        }
        //Wait for next pulse (there must be a better way!)
        while read_bar(&mut bar_file, TOTAL_PULSE_OFFSET)
            .context("Failed to read Total Pulse")? == total_pulse {
                std::hint::spin_loop();
        }
        if main_loop_counter % 400 == 0 {
            println! {"Pulse number: {}. Time around the loop: {} us",
                      total_pulse, shot_start.elapsed().as_micros()};
        }
        main_loop_counter += 1;

    }

    //Handle closing
    drop(datasender);
    //Dropping the datasender should hangup
    match heartbeatsender.try_send(true) {
        Ok(()) => {
            println!("Shutting down heartbeat thread");
            heartbeat_handle.join()
                .expect("Heartbeat thread is already dead");
        }
        Err(TrySendError::Full(_)) => {
            println!("Shutting down heartbeat thread");
            heartbeat_handle.join()
                .expect("Heartbeat thread is already dead");
        }
        Err(TrySendError::Disconnected(_)) => {
            //Heartbeat thread is already dead, no need to do anything
        }
    }
    write_handle.join().expect("Write thread is already dead");
    //Join write thread to wait for shutdown
    println!("SHUTDOWN: {}", main_loop_counter);
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
    buffer.seek(SeekFrom::Start(offset))
        .with_context(|| format!("Error while seeking to {} in {:?}", offset, buffer))?;
    buffer.read_u16_into::<LittleEndian>(&mut output)
        .with_context(|| format!("Error while reading {:?} into {:?}", buffer, output))?;
    Ok(output)
}


fn write_thread (receiver: Receiver<DataContainer>) -> Result<()> {
    let mut write_count = 0;
    let mut rolling_avg:Vec<i64> = Vec::new();
    let mut write_time:i64 = 0;
    let mut write_start = time::Instant::now();
    let mut next_midnight = Utc::now()
        .date()
        .succ()
        .and_hms(0,0,0);

    let mut hdffname = TMP_LOC.to_owned() + &chrono::Utc::now()
        .format("%Y-%m-%d %H:%M:%S.%f.h5")
        .to_string();
    hdf5::File::create(&hdffname)
        .context("Failed to open hdffile")?;

    //let mut bin_write = File::create(TMP_LOC.to_owned() + "binfile")
    //    .context("Failed to open binfile")?;


    loop{
        match receiver.recv_timeout(time::Duration::from_micros(2550)) {
            Ok(data) => {
                //Received data, write it to file
                let total_pulse = data.total_pulse;

                write_hdf(&hdffname, data)
                    .context("Failed to write binary file")?;

                write_time = write_start.elapsed().as_micros() as i64;

                rolling_avg.push(write_time);

                if write_count % 400 == 0 {
                    println!("Fin, took {} us to write pulse {}", write_time, total_pulse);
                    let sum: i64 = rolling_avg.iter().sum();
                    let len: i64 = rolling_avg.len() as i64;
                    println!("Rolling avg is {} us", sum/len);
                    rolling_avg.clear();
                }
                write_count += 1;
                write_start = time::Instant::now();
            }
            Err(RecvTimeoutError::Timeout) => {
                //Took longer than 1000 us to receive data. Restart the loop, but don't worry about it
                if write_count % 400 == 0 {
                    println!("Receive timeout");
                }
            }
            Err(RecvTimeoutError::Disconnected) => {
                //Main thread has disconnected, probably indicates that we should stop writing and return
                break;
            }
        }
        if Utc::now() > next_midnight {
            //Switch to next hdf5file
            hdffname = TMP_LOC.to_owned() + &chrono::Utc::now()
                .format("%Y-%m-%d %H:%M:%S.%f.h5")
                .to_string();
            hdf5::File::create(&hdffname)
                .context("Failed to open hdffile")?;
            next_midnight = Utc::now()
                .date()
                .succ()
                .and_hms(0,0,0);

        }
    }
    println!("Write thread: {}", write_count);
    println!("Rolling avg is {:?} us", rolling_avg);
    Ok(())
}

fn write_binary(buffer: &mut File, data: DataContainer) -> Result<()> {
    buffer.write_u64::<LittleEndian>(data.internal_count)
        .with_context(|| format!("Failed to write internal count ({})", data.internal_count))?;

    buffer.write_u64::<LittleEndian>(data.datetime.timestamp_nanos() as u64)
        .with_context(|| format!("Failed to write timestamp ({})", data.datetime.timestamp_nanos() as u64))?;
    
    buffer.write_u64::<LittleEndian>(data.active_pulse)
        .with_context(|| format!("Failed to write active pulse ({})", data.active_pulse))?;

    buffer.write_u64::<LittleEndian>(data.total_pulse)
        .with_context(|| format!("Failed to write total pulse ({})", data.total_pulse))?;

    buffer.write_u64::<LittleEndian>(data.state as u64)
        .with_context(|| format!("Failed to write state ({})", data.state as u64))?;
    let mut i = 0;
    for array in data.into_iter() {
        for ii in 0..128 {
            buffer.write_u16::<LittleEndian>(array[ii])
                .with_context(|| format!("Failed to write sample {} of {}", ii, DATA_FIELD_NAMES[i]))?;
        }
        i += 1;
    }
    Ok(())
}


fn write_hdf(hdffname: &str, data: DataContainer) -> Result<()> {
    let mut hdffile = hdf5::File::open_rw(&hdffname, )
        .context("Failed to open hdffile")?;
    let timestamp = &data.datetime.format("%Y-%m-%d %H:%M:%S.%f").to_string();
    let hdfgroup = hdffile
        .create_group(timestamp)
        .with_context(|| format!("Failed to create group at {}", timestamp))?;

    //Write attributes
    write_hdf_attr(&hdfgroup, "internal_count", data.internal_count)
        .context("Failed to call write_hdf_attr for internal_count")?;

    write_hdf_attr(&hdfgroup, "datetime", data.datetime.timestamp_nanos() as u64)
        .context("Failed to call write_hdf_attr for datetime")?;

    write_hdf_attr(&hdfgroup, "active_pulse", data.active_pulse)
        .context("Failed to call write_hdf_attr for active_pulse")?;

    write_hdf_attr(&hdfgroup, "total_pulse", data.total_pulse)
        .context("Failed to call write_hdf_attr for total_pulse")?;

    write_hdf_attr(&hdfgroup, "state", data.state as u64)
        .context("Failed to call write_hdf_attr for state")?;
    let mut i = 0;
    for array in data.into_iter() {
        write_hdf_ds(&hdfgroup, DATA_FIELD_NAMES[i], &array)
            .with_context(|| format!("Failed to call write_hdf_arr for {}", DATA_FIELD_NAMES[i]))?;
        i += 1;
    }
    hdffile.flush()
        .context("Failed to flush hdffile")?;
    hdffile.close()
        .context("Failed to close hdffile")?;
    Ok(())
}

fn write_hdf_attr(hdfgroup: &hdf5::Group, name: &str, data: u64) -> Result<()> {
    let attr = hdfgroup
        .new_attr::<u64>()
        .shape([1])
        .create(name)
        .with_context(|| format!("Unable to create attr {}", name))?;
    attr
        .write(&[data])
        .with_context(|| format!("Unable to write {} to attr {}", data, name))?;
    Ok(())
}

fn write_hdf_ds(hdfgroup: &hdf5::Group, name: &str, data: &[u16; SAMPLES]) -> Result<()> {
    let builder = hdfgroup
        .new_dataset_builder()
        .chunk(CHUNK_SIZE);

    builder
        .with_data(data)
        .create(name)
        .with_context(|| format!("Failed to write dataset {}", name))?;

    Ok(())
}

fn switch_hdf() {

}


fn false_heartbeat(pulse_rate: Duration, ctrl: Receiver<bool>) -> Result<()>{
    let mut pulse_counter: u64 = 0;

    let mut pfile = OpenOptions::new()
        .write(true)
        .open(BAR1_NAME).with_context(|| format!("Fake heartbeat could not open {} in write mode", BAR1_NAME))?;

    loop {
        let end_at = time::Instant::now() + pulse_rate;
        pfile.seek(SeekFrom::Start(TOTAL_PULSE_OFFSET)).context("Error while seeking in fake heartbeat")?;
        pfile.write_u64::<LittleEndian>(pulse_counter).with_context(|| format!("Failed to write {} to {}", pulse_counter, BAR1_NAME))?;
        pulse_counter = pulse_counter.wrapping_add(1);

        while time::Instant::now() < end_at {
            std::hint::spin_loop();
        }
        if pulse_counter % 400 == 0 {
            println! {"Finished a heartbeat!"};
        }

        match ctrl.try_recv() {
            Ok(_) => {
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
