import subprocess
from unittest import result
from pdf2image import convert_from_path

file_path = input("Enter the path to the PDF file: ")
save_to_gdrive = input("Do you want to save the image to Google Drive? (yes/no): ").lower()
filename = file_path.split("/")[-1].split(".")[0]
images = convert_from_path(file_path, dpi=500)

if save_to_gdrive == "yes":
    result = subprocess.run([
        "rclone", "copy", f"{filename}.png",
        "gdrive:PhD-Work/2_PhD Paper 2/Tables/"],
            capture_output=True, text=True)
    if result.returncode == 0:
        print("Upload successful")
    else:
        print(f"Error: {result.stderr}")
else:
    images[0].save(f"{filename}.png", "PNG")
    print(f"Image saved locally as {filename}.png")