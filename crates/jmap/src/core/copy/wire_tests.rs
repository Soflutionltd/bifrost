use crate::core::SetCreate;
use crate::core::test_support::*;

#[test]
fn copy_payload_retains_its_caller_selected_creation_id() {
    let mut copy = crate::core::copy::CopyRequest::<TestObj>::new("source");
    assert_eq!(copy.create("copy0").create_id().as_deref(), Some("copy0"));
}
