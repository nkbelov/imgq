send:
    curl -X POST http://localhost:3000/enqueue \
    -F "image=@/Users/wtedst/Desktop/3lp" \
    -F 'transform={"Resize":{"width":800,"height":600}}'
