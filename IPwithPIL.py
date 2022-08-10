from PIL import Image
import os

origin = "images"
flist = [f for f in os.listdir(origin) if os.path.isfile(os.path.join(origin, f))]
new = "transformed"
print(flist)
for file in flist:
    if file[0] == '.':
        continue
    im = Image.open(os.path.join(origin, file))
    im.resize((128,128)).convert('RGB').rotate(90).save(os.path.join(new, file), format="JPEG")