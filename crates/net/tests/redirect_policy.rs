use bifrost_net::RedirectPolicy;

#[test]
fn same_origin_policy_distinguishes_host_filter_from_hop_admission() {
    let policy = RedirectPolicy::default()
        .same_origin()
        .trust_host("other.example");
    let prior = reqwest::Url::parse("https://dav.example/a").unwrap();
    assert!(!policy.allows_host("dav.example"));
    assert!(policy.admits_hop(Some(&prior), &prior.join("/b").unwrap()));
    assert!(policy.admits_hop(
        Some(&prior),
        &reqwest::Url::parse("https://dav.example:443/b").unwrap(),
    ));
    for next in [
        "http://dav.example/b",
        "https://dav.example:444/b",
        "https://other.example/b",
    ] {
        assert!(!policy.admits_hop(Some(&prior), &reqwest::Url::parse(next).unwrap(),));
    }
    assert!(!policy.admits_hop(None, &prior));
}
