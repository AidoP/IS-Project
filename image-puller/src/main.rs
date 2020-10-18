use std::{fs::File, io::{self,BufRead,BufReader}};
use std::path::PathBuf;
use std::collections::HashMap;
//use tokio::{self,fs::File,io::{self,AsyncWriteExt,AsyncRead,AsyncBufRead,AsyncBufReadExt,BufReader}};
use reqwest;
use image;

const REQUIRED_CONFIDENCE: f32 = 0.5;

struct BoundingBox<'a> {
    label: &'a str,
    confidence: f32,
    points: (f32, f32, f32, f32)
}

#[tokio::main]
async fn main() -> io::Result<()> {
    let labels_file = BufReader::new(File::open("sources/labels.csv").unwrap());
    let categories_file = BufReader::new(File::open("sources/categories.csv").unwrap());
    let boxes_file = BufReader::new(File::open("sources/bounding-boxes.csv").unwrap());
    let images_file = BufReader::new(File::open("sources/images.csv").unwrap());

    
    // Find man and woman labels
    let (woman_label, man_label) = {
        let (mut woman_label, mut man_label) = (None, None);
        for line in labels_file.lines() {
            let line = line?;
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
    
    // Get image bounding boxes
    let bounding_boxes = {
        let mut boxes = HashMap::<String, Vec::<_>>::new();
        for line in boxes_file.lines() {
            let line = line?;
            let mut parts = line.split(",");
            let id = parts.next().map(|s| s.to_owned()).expect("Invalid image id in boxes file");
            let mut parts = parts.skip(1);
            let label = parts.next().expect("Invalid boxes file");
            let new_box = BoundingBox {
                label: match label {
                    label if label == woman_label => &woman_label,
                    label if label == man_label => &man_label,
                    _ => continue
                },
                confidence: parts.next().expect("Invalid boxes file").parse().expect("Confidence is not a float"),
                points: (
                    parts.next().expect("Invalid boxes file").parse().expect("XMin is not a float"),
                    parts.next().expect("Invalid boxes file").parse().expect("XMax is not a float"),
                    parts.next().expect("Invalid boxes file").parse().expect("YMin is not a float"),
                    parts.next().expect("Invalid boxes file").parse().expect("YMax is not a float")
                )
            };
            if let Some(boxes) = boxes.get_mut(&id) {
                boxes.push(new_box)
            } else {
                boxes.insert(id, vec![new_box]);
            }
        }
        boxes
    };

    // Get images links
    let images_future = get_images(images_file);
    // Get image ids
    let ids_future = get_ids(categories_file, &woman_label, &man_label);

    let images = images_future.await?;
    let (women, men) = ids_future.await?;
    
    save_all(&woman_label, &man_label, women.into_iter().chain(men.into_iter()), &images, &bounding_boxes).await.unwrap_or_else(|e| eprintln!("Error getting image: {:?}", e));
    //save_all(men, &images, &bounding_boxes).await.unwrap_or_else(|e| eprintln!("Error getting image: {:?}", e));

    Ok(())
}

async fn get_images(images_file: BufReader<File>) -> io::Result<HashMap<String, String>> {
    let mut images = HashMap::new();
    for line in images_file.lines() {
        let line = line?;
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
    for line in categories_file.lines() {
        let line = line?;
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

async fn save_all<'a>(woman_label: &str, man_label: &str, ids: impl Iterator<Item=String>, images: &HashMap<String, String>, bounding_boxes: &HashMap<String, Vec<BoundingBox<'a>>>) -> reqwest::Result<()> {
    for image_id in ids {
        if let Some(boxes) = bounding_boxes.get(&image_id) {
            let image_request = reqwest::get(&images[&image_id]);
            let mut path = PathBuf::new();
            path.push("images");

            use image::GenericImageView;
            match image::load_from_memory(&*image_request.await?.bytes().await?) {
                Ok(image) => {
                    let (width, height) = image.dimensions();
                    let (width, height) = (width as f32, height as f32);
                    for (index, bounding_box) in boxes.iter().enumerate() {
                        // Skip if a bad photo
                        if bounding_box.confidence < REQUIRED_CONFIDENCE { continue }

                        let mut path = path.clone();
                        path.push(match bounding_box.label {
                            label if label == woman_label => "women",
                            label if label == man_label => "men",
                            _ => panic!("This label should be pruned")
                        });
                        let (x, y) = (bounding_box.points.0 * width, bounding_box.points.2 * height);
                        let (width, height) = (bounding_box.points.1 * width - x, bounding_box.points.1 * width - x);
                        let image = image.crop_imm(x as _, y as _, width as _, height as _);
                        path.push(&format!("{}-{}.png", image_id, index));
                        image.save(path).unwrap_or_else(|e| eprintln!("Unable to save image: {}", e));
                    }
                }
                Err(error) => eprintln!("Unable to load image: {:?}", error)
            }
        }
    }

    Ok(())
}