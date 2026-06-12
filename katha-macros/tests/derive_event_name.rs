#[test]
fn derive_event_name_ui() {
    let t = trybuild::TestCases::new();
    t.pass("tests/ui/event_name_default_pass.rs");
    t.pass("tests/ui/event_name_explicit_pass.rs");
}
