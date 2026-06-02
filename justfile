send:
    curl -X POST http://localhost:3000/enqueue \
    -F "image=@3lp.png" \
    -F 'transform={"Resize":{"width":800,"height":600}}'
