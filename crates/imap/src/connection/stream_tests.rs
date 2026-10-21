use bytes::BytesMut;
use flate2::{Decompress, FlushDecompress, Status};
use tokio::io::AsyncReadExt;

use super::{CompressedStream, InnerStream, Tracked, ensure_deflate_progress};

#[test]
fn deflate_no_progress_is_a_write_zero_error() {
    let err = ensure_deflate_progress(0, 0).expect_err("no progress must fail");
    assert_eq!(err.kind(), std::io::ErrorKind::WriteZero);
}

#[tokio::test]
async fn compressed_stream_writes_a_raw_deflate_byte_stream() {
    let (client, mut server) = tokio::io::duplex(1024);
    let mut stream = CompressedStream::new(InnerStream::Memory(Tracked::new(client)));
    let plaintext = b"P001 NOOP\r\n";

    stream.write_all(plaintext).await.expect("compressed write");
    stream.flush().await.expect("inner flush");

    let mut raw = vec![0_u8; 1024];
    let read = server.read(&mut raw).await.expect("read compressed bytes");
    assert!(read > 0, "SyncFlush must write compressed bytes");

    let mut inflater = Decompress::new(false);
    let mut inflated = BytesMut::new();
    let mut scratch = vec![0_u8; 1024];
    let status = inflater
        .decompress(&raw[..read], &mut scratch, FlushDecompress::Sync)
        .expect("raw deflate payload");
    inflated.extend_from_slice(&scratch[..usize::try_from(inflater.total_out()).unwrap()]);

    assert!(matches!(status, Status::Ok | Status::BufError));
    assert_eq!(&inflated[..], plaintext);
}

#[tokio::test]
async fn compressed_stream_reuses_deflate_scratch_storage_across_writes() {
    let (client, _server) = tokio::io::duplex(1024);
    let mut stream = CompressedStream::new(InnerStream::Memory(Tracked::new(client)));
    let scratch = stream.deflate_buf.as_ptr();

    stream
        .write_all(b"P001 NOOP\r\n")
        .await
        .expect("first write");
    assert_eq!(stream.deflate_buf.as_ptr(), scratch);
    stream
        .write_all(b"P002 NOOP\r\n")
        .await
        .expect("second write");

    assert_eq!(stream.deflate_buf.as_ptr(), scratch);
    assert_eq!(stream.deflate_buf.len(), super::DEFLATE_BUF_SIZE);
}
