import chardet

file_path = "bezvo.csv"

# Lese nur ein Sample, das reicht meist
with open(file_path, "rb") as f:
    rawdata = f.read(10000)  # first 10 KB
    result = chardet.detect(rawdata)
    print(result)