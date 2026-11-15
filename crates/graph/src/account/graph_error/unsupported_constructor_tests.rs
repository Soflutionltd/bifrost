use super::*;

#[test]
fn unsupported_constructor_stamps_protocol_and_kind() {
    let operation = bifrost_types::AccountOperation::Send;
    let protocol = bifrost_types::Protocol::Graph;
    let error = unsupported_account_error(operation);
    assert_eq!(error.protocol(), Some(protocol));
    assert_eq!(error.operation(), Some(operation));
    assert_eq!(
        error.kind(),
        &bifrost_types::AccountErrorKind::Unsupported(operation)
    );
}
