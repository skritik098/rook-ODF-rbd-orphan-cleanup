# How to Share Scripts via gzip + base64 Encoding

## Sender Side (Encode)

### Step 1: Compress and encode the script
```bash
gzip -c rbd_tree_builder.py | base64 -w0 > rbd_tree_builder.b64
gzip -c rbd_cleanup.py | base64 -w0 > rbd_cleanup.b64
```

- `gzip -c` — compresses the file, outputs to stdout (original file untouched)
- `base64 -w0` — encodes binary to text with no line wrapping
- Result: a single line of text safe to paste anywhere (chat, email, ticket)

### Step 2: Check the encoded size
```bash
wc -c rbd_tree_builder.b64 rbd_cleanup.b64
```

### Step 3: Share the encoded string
Copy the content of the `.b64` file and share it along with the decode command below.

```bash
cat rbd_tree_builder.b64
```

---

## Recipient Side (Decode)

### Option A: Paste directly in terminal
```bash
echo "<paste_entire_encoded_string_here>" | base64 -d | gunzip > rbd_tree_builder.py
echo "<paste_entire_encoded_string_here>" | base64 -d | gunzip > rbd_cleanup.py
```

### Option B: Save encoded string to file first, then decode
```bash
# Save the encoded text to a file
cat > rbd_tree_builder.b64 << 'EOF'
<paste_entire_encoded_string_here>
EOF

# Decode
base64 -d rbd_tree_builder.b64 | gunzip > rbd_tree_builder.py
```

### Step: Verify
```bash
head -5 rbd_tree_builder.py
python3 rbd_tree_builder.py --help
```

---

## What each command does

| Command | Purpose |
|---|---|
| `gzip -c <file>` | Compress file → stdout (~70% size reduction) |
| `base64 -w0` | Binary → text (safe for copy-paste), no line wraps |
| `base64 -d` | Text → binary (reverse of encode) |
| `gunzip` | Decompress → original file |