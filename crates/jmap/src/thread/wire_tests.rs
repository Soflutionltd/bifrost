mod object_wire {
    #[test]
    fn thread_partial_projections_decode() {
        let thread: crate::thread::Thread =
            serde_json::from_str(r#"{"id":"t1","emailIds":["e1","e2"]}"#).expect("thread decodes");
        assert_eq!(thread.email_ids().expect("emailIds requested").len(), 2);

        let partial: crate::thread::Thread =
            serde_json::from_str(r#"{"id":"t1"}"#).expect("partial projection decodes");
        assert!(partial.email_ids().is_none());

        // JMAP always includes an object's id in a /get response, even
        // when the requested projection excludes it.
        assert!(serde_json::from_str::<crate::thread::Thread>(r#"{"emailIds":["e1"]}"#).is_err());
    }
}
