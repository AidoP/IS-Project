//use std::{fs::File, io::{self,Read}, path::Path};
use std::path::PathBuf;
use std::collections::{HashMap, VecDeque};
use tokio::{self,fs::File,io::{self,AsyncWriteExt,AsyncRead,AsyncBufRead,AsyncBufReadExt,BufReader}};
use reqwest;

const IMAGE_GET_REQUESTS: usize = 3;

#[tokio::main]
async fn main() -> io::Result<()> {
    let labels_file = BufReader::new(File::open("sources/labels.csv").await?);
    let categories_file = BufReader::new(File::open("sources/categories.csv").await?);
    let boxes_file = BufReader::new(File::open("sources/bounding-boxes.csv").await?);
    let images_file = BufReader::new(File::open("sources/images.csv").await?);

    
    // Find man and woman labels
    let (woman_label, man_label) = {
        let mut lines = labels_file.lines();
        let (mut woman_label, mut man_label) = (None, None);
        while let Some(line) = lines.next_line().await? {
            let mut parts = line.split(",");
            let category = parts.next().map(|s| s.to_owned());
            let label = parts.next().expect("Invalid labels file");
            match label {
                "Woman" => woman_label = category,
                "Man" => man_label = category,
                _ => ()
            }
        }
        (woman_label.expect("Unable to find woman label"), man_label.expect("Unable to find man label"))
    };

    // Get images links
    let images_future = get_images(images_file);
    // Get image ids
    let ids_future = get_ids(categories_file, &woman_label, &man_label);

    let images = images_future.await?;
    let (women, men) = ids_future.await?;
    
    save_all(women, &images).await.unwrap_or_else(|e| eprintln!("Error getting image: {:?}", e));
    save_all(men, &images).await.unwrap_or_else(|e| eprintln!("Error getting image: {:?}", e));

    Ok(())
}

async fn get_images(images_file: BufReader<File>) -> io::Result<HashMap<String, String>> {
    let mut images = HashMap::new();
    let mut lines = images_file.lines();
    while let Some(line) = lines.next_line().await? {
        let mut parts = line.split(",");
        if let Some(_) = images.insert(
            parts.next().unwrap().to_owned(),
            parts.skip(1).next().unwrap().to_owned()
        ) {
            panic!("Image ID exists more than once")
        }
    }
    Ok(images)
}
async fn get_ids(categories_file: BufReader<File>, woman_label: &str, man_label: &str) -> io::Result<(Vec<String>, Vec<String>)> {
    let mut ids = (vec![], vec![]);
    let mut lines = categories_file.lines();
    while let Some(line) = lines.next_line().await? {
        let mut parts = line.split(",");
        let id = parts.next().unwrap().to_owned();
        match parts.skip(1).next().unwrap() {
            label if label == woman_label => ids.0.push(id),
            label if label == man_label => ids.1.push(id),
            _ => ()
        }
    }
    Ok(ids)
}

async fn save_all(ids: Vec<String>, images: &HashMap<String, String>) -> reqwest::Result<()> {
    let mut get_futures = VecDeque::new();
    for image in ids {
        get_futures.push_back((image.clone(), reqwest::get(&images[&image])));
        if get_futures.len() > IMAGE_GET_REQUESTS {
            let (mut id, image) = get_futures.pop_front().unwrap();
            let mut path = PathBuf::new();
            path.push("images");
            id.push_str(".png");
            path.push(id);
            println!("Saving image {:?}", path);
            let mut file = File::create(path).await.expect("Unable to create image file");
            file.write_all(&*image.await?.bytes().await?).await.expect("Unable to write to image file");
        }
    }

    Ok(())
}