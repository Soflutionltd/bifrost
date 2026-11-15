use crate::account::pim::common::graph_extended_property_id;
use crate::account::pim::messages::PR_LAST_VERB_EXECUTED_GRAPH_ID;

#[test]
fn last_verb_executed_aliases_map_onto_the_graph_proptag() {
    // Replied and forwarded shapes select `ExtendedProperty`
    // are advertised true, and the convenience layer passes the MAPI
    // alias; Graph only accepts the proptag form.
    for alias in [
        "PR_LAST_VERB_EXECUTED",
        "pr_last_verb_executed",
        "PidTagLastVerbExecuted",
        "pidtaglastverbexecuted",
    ] {
        assert_eq!(
            graph_extended_property_id(alias),
            PR_LAST_VERB_EXECUTED_GRAPH_ID
        );
    }
    // Anything else passes through verbatim - the caller owns the id.
    assert_eq!(
        graph_extended_property_id("SystemTime 0x3FEF"),
        "SystemTime 0x3FEF"
    );
}
