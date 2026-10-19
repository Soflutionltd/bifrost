use super::{
    EwsBodyStream, EwsClient, EwsError, EwsExecute, EwsHeaders, build_soap_envelope,
    check_response_error, check_soap_fault,
};
use futures::TryStreamExt;

impl EwsExecute for EwsClient {
    async fn execute(&self, body_xml: &str, headers: &EwsHeaders) -> Result<String, EwsError> {
        // Inherent methods outrank trait methods in resolution, so this
        // delegates rather than recursing.
        EwsClient::execute(self, body_xml, headers).await
    }

    async fn execute_streaming(
        &self,
        body_xml: &str,
        headers: &EwsHeaders,
    ) -> Result<EwsBodyStream, EwsError> {
        // EWS throttling is per calling user under its own policy; streaming
        // notifications have a separate subscription budget. The REST
        // per-mailbox four does not apply, so this path stays ungated.
        let envelope = build_soap_envelope(body_xml);
        let mut req = self
            .net
            .post(self.endpoint()?)
            .header("Content-Type", "text/xml; charset=utf-8");
        for (name, value) in headers.pairs() {
            req = req.header(name, &value);
        }
        let response = req
            .body(bytes::Bytes::from(envelope))
            .send_streaming()
            .await
            .map_err(EwsError::Transport)?;
        // Do not enroll this long-lived stream in batch accounting. Its
        // counter is only complete if the caller drains it, and publishing
        // a partial count as a batch total would overstate its completeness.
        Ok(Box::pin(response.body.map_err(EwsError::Transport)))
    }
}

impl EwsClient {
    /// Build a client over the account's admitted EWS endpoint (see
    /// `GraphClient::ews_url`), or over the reason there is none, in which
    /// case every request fails closed.
    pub(crate) fn new(
        net: bifrost_net::AccountNet,
        ews_url: Result<crate::origin::AdmittedUrl, String>,
    ) -> Self {
        Self {
            net,
            ews_url,
            tally: None,
        }
    }

    /// The endpoint every request carries the bearer to.
    fn endpoint(&self) -> Result<&str, EwsError> {
        self.ews_url
            .as_ref()
            .map(crate::origin::AdmittedUrl::as_str)
            .map_err(|reason| EwsError::Configuration(reason.clone()))
    }

    /// Enroll buffered SOAP responses in an existing batch accumulator.
    pub(crate) fn with_tally(mut self, tally: Option<crate::client::ByteTally>) -> Self {
        self.tally = tally;
        self
    }

    /// Execute a raw EWS SOAP request. Wraps `body_xml` in the SOAP
    /// envelope, sends it, checks for SOAP faults, and returns the
    /// response body as a string.
    ///
    /// Transport failures preserve the `bifrost_net::Error` so the
    /// account boundary classifies them through
    /// `bifrost_net::into_account_error` with `Protocol::Ews` context.
    /// HTTP error statuses carry the status + raw body; SOAP faults
    /// carry the structured `SoapFaultCode` plus `<faultstring>`
    /// detail.
    pub(crate) async fn execute(
        &self,
        body_xml: &str,
        headers: &EwsHeaders,
    ) -> Result<String, EwsError> {
        // EWS throttling is per calling user under its own policy; streaming
        // notifications have a separate subscription budget. The REST
        // per-mailbox four does not apply, so this path stays ungated.
        let envelope = build_soap_envelope(body_xml);
        let mut req = self
            .net
            .post(self.endpoint()?)
            .header("Content-Type", "text/xml; charset=utf-8");
        for (name, value) in headers.pairs() {
            req = req.header(name, &value);
        }
        // The counter is caller-owned so the bytes survive an `Err`.
        // `AccountNet` drains non-2xx bodies, exhausted retries and
        // repeated 401s before converting them to `Error`, and the
        // hydration arm turns those errors into per-item failures while
        // still emitting a batch - so a success-path-only tally write
        // would report that batch's total as zero despite real traffic.
        let counter = bifrost_net::RequestByteCounter::new();
        let sent = req
            .body(bytes::Bytes::from(envelope))
            .count_bytes_into(counter.clone())
            .send()
            .await;
        if let Some(tally) = self.tally.as_ref() {
            tally.add(counter.bytes_in());
        }
        let resp = sent.map_err(EwsError::Transport)?;

        let status = resp.status();
        if !status.is_success() {
            return Err(EwsError::HttpStatus {
                status,
                body: resp.body,
            });
        }

        let xml = String::from_utf8_lossy(resp.body.as_ref()).into_owned();
        // Two distinct error shapes ride inside a 200 OK: a SOAP
        // `<Fault>` (transport/envelope failure) and the canonical EWS
        // application error `ResponseClass="Error"` / `<m:ResponseCode>`.
        // Inspect both before any parser sees the body, so an
        // `ErrorAccessDenied` / `ErrorServerBusy` is classified instead
        // of degrading to an empty success.
        check_soap_fault(&xml)?;
        check_response_error(&xml)?;
        Ok(xml)
    }
}
