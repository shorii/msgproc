#[macro_export]
macro_rules! kafka_error {
    ($addr: ident, $e: expr) => {
        $addr.do_send($crate::msg::MsgProcResult(Err(
            $crate::error::MsgProcError::KafkaError($e.to_string()),
        )));
    };
}

#[macro_export]
macro_rules! handle_error {
    ($addr: ident, $e: expr) => {
        $addr.do_send($crate::msg::MsgProcResult(Err(
            $crate::error::MsgProcError::HandleError($e.to_string()),
        )));
    };
}
