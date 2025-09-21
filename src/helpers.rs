pub fn build_chunks(start: u64, end: u64, chunk_size: u64) -> Vec<(u64, u64)> {
    let mut chunks = Vec::with_capacity((end - start + 1) as usize / chunk_size as usize);
    let mut start = start;

    while start <= end {
        let end = (start + chunk_size - 1).min(end);
        chunks.push((start, end));
        start += chunk_size;
    }

    chunks
}
