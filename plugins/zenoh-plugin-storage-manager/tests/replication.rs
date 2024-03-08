//!
//! Proptest state machine testing Zenoh Router replication.
//!
//! Each transition can do one of a number of operations
//! 1. Start a router
//! 2. Restart a previously started and stopped one router w/o deleting its storage ddata
//! 3. Stop one of the routers
//! 4. Create a key
//! 5. Update an existing key
//!
//! After each transition, the `apply` function checks the condition of
//! the various routers to make sure they're consistent and reflect what's
//! stored in the RefState. This can require waiting for some amount of
//! time to allow items to propagate between the DBs (if there are
//! multiple).
//!
//!
//! To run this test launch with the environment variable
//! ZENOH_INSTALL_PATH set to the path to a single directory
//! containing:
//! - zenohd
//! - libzenoh_backend_fs.so/dylib
//! - libzenoh_plugin_rest.so/dylib
//! - libzenoh_plugin_storage_manager.so/dylib
//!
//! This ensure the subprocess launches with the correct information.
//!
use log::*;
use proptest::{prelude::*, test_runner::Config};
use proptest_state_machine::{
    prop_state_machine, ReferenceStateMachine, StateMachineTest,
};
use std::{
    collections::{HashMap, HashSet},
    io::Write,
    path::{Path, PathBuf},
    process::{Child, Command},
    time::{Duration, Instant},
};
use tempfile::{tempdir, TempDir};
use zenoh::prelude::{sync::SyncResolve, *};

const TEST_PREFIX: &str = "test-keys";

prop_state_machine! {
    #![proptest_config(Config {
        // Enable verbose mode to make the state machine test print the
        // transitions for each case.
        verbose: 2,
        // Only run 10 cases by default to avoid running out of system resources
        // and taking too long to finish.
        cases: 15,

        // Uncomment this line to avoid attempting to shrink at all.
        max_shrink_time: 1,
        .. Config::default()
    })]

    #[test]
    fn run_zenoh_replication_test(
        // This is a macro's keyword - only `sequential` is currently supported.
        sequential
        // The number of transitions to be generated for each case. This can
        // be a single numerical value or a range as in here.
        1..50
        // Macro's boilerplate to separate the following identifier.
        =>
        // The name of the type that implements `StateMachineTest`.
        StorageReplicationTest
    );
}

type RouterId = usize;

/// The possible transitions of the state machine.
#[derive(Clone, Debug, strum_macros::Display)]
enum Transition {
    StartRouter(RouterId),
    RestartRouter(RouterId),
    StopRouter(RouterId),
    CreateKey(RouterId, OwnedKeyExpr, String),
    UpdateKey(RouterId, OwnedKeyExpr, String),
}

/// The reference state of the server and clients.
#[derive(Clone, Debug, Default)]
struct RefState {
    router: HashSet<RouterId>,
    stopped_router: HashSet<RouterId>,

    // All keys along with their value. None means it was deleted
    resources: HashMap<OwnedKeyExpr, Option<String>>,
}

/// We don't handle UTF-8 correctly in loading and storing so we just
/// settle for ASCII strings for now.
fn ascii_string() -> impl Strategy<Value = String> {
    proptest::string::string_regex("[0-9A-Za-z_]+").unwrap()
}

fn arb_key_expr() -> impl Strategy<Value = OwnedKeyExpr> {
    proptest::string::string_regex(&format!("{TEST_PREFIX}/[0-9A-Za-z]+"))
        .unwrap()
        .prop_map(|s| s.try_into().unwrap())
}

impl ReferenceStateMachine for RefState {
    type State = RefState;
    type Transition = Transition;

    fn init_state() -> BoxedStrategy<Self::State> {
        Just(RefState::default()).boxed()
    }

    fn transitions(state: &Self::State) -> BoxedStrategy<Self::Transition> {
        use Transition::*;

        let start_new =
            Just(StartRouter(state.router.len() + state.stopped_router.len())).boxed();
        let restart_deleted = proptest::sample::select(
            state.stopped_router.iter().copied().collect::<Vec<_>>(),
        )
        .prop_map(RestartRouter)
        .boxed();

        let rid =
            proptest::sample::select(state.router.iter().copied().collect::<Vec<_>>());
        let stop_existing = rid.clone().prop_map(StopRouter).boxed();

        let router_statusers = {
            if state.stopped_router.is_empty() {
                // If we've never stopped any ROUTER then we can only start
                start_new
            } else if state.router.len() <= 1 {
                // If there is only 1 running then we need to restart one
                // so that we have something to sync from
                restart_deleted.boxed()
            } else {
                // If we have some stopped and some existing, then we can do either
                start_new.prop_union(restart_deleted).boxed()
            }
        };

        let router_statusers = if state.router.len() >= 2 {
            router_statusers.prop_union(stop_existing).boxed()
        } else {
            router_statusers
        };

        if !state.router.is_empty() {
            let key_id = proptest::sample::select(
                state.resources.keys().cloned().collect::<Vec<_>>(),
            );

            let strat = (rid.clone(), arb_key_expr(), ascii_string())
                .prop_map(|(rid, key, value)| CreateKey(rid, key, value))
                .boxed();

            let key_update = if !state.resources.is_empty() {
                // Need to guarantee something exists before renaming
                strat
                    .prop_union(
                        (rid.clone(), key_id.clone(), ascii_string())
                            .prop_map(|(rid, key, value)| {
                                UpdateKey(rid, key.clone(), value)
                            })
                            .boxed(),
                    )
                    .boxed()
            } else {
                strat.boxed()
            };
            router_statusers.prop_union(key_update).boxed()
        } else {
            router_statusers
        }
    }

    fn apply(mut state: Self::State, transition: &Self::Transition) -> Self::State {
        info!("Transition: {}", transition);
        use Transition::*;
        match transition {
            StartRouter(id) => {
                state.router.insert(*id);
            }
            StopRouter(id) => {
                state.router.remove(id);
                state.stopped_router.insert(*id);
            }
            RestartRouter(id) => {
                state.stopped_router.remove(id);
                state.router.insert(*id);
            }
            CreateKey(_rid, key, value) => {
                state.resources.insert(key.clone(), Some(value.clone()));
            }
            UpdateKey(_rid, key, name) => {
                state.resources.get_mut(key).unwrap().replace(name.clone());
            }
        }
        state
    }

    fn preconditions(state: &Self::State, transition: &Self::Transition) -> bool {
        use Transition::*;
        match transition {
            StartRouter(id) => !state.router.contains(id),
            StopRouter(id) => state.router.contains(id),
            RestartRouter(id) => state.stopped_router.contains(id),
            CreateKey(rid, _key, _) => state.router.contains(rid),
            UpdateKey(rid, key, _) => {
                state.router.contains(rid)
                    && state
                        .resources
                        .get(key)
                        .map(Option::is_some)
                        .unwrap_or(false)
            }
        }
    }
}

struct StorageReplicationTest {
    router: HashMap<RouterId, Option<TestRouter>>,
    storage_dir: StorageDir,
}

#[allow(dead_code)]
enum StorageDir {
    Temp(TempDir),
    Path(PathBuf),
}

impl StorageDir {
    fn path(&self) -> &Path {
        match self {
            StorageDir::Temp(t) => t.path(),
            StorageDir::Path(p) => &p,
        }
    }
}

struct TestRouter {
    // A client session only connected to the specific router.
    sesh: Session,
    _router: ZenohRouterProcess,
}

struct ZenohRouterProcess(Child);

impl Drop for ZenohRouterProcess {
    fn drop(&mut self) {
        info!("Dropping zenoh proc");
        let _ = self.0.kill();
    }
}

impl StateMachineTest for StorageReplicationTest {
    type SystemUnderTest = Self;
    type Reference = RefState;

    fn init_test(ref_state: &RefState) -> Self::SystemUnderTest {
        Self::init_test(ref_state)
    }

    fn apply(s: Self, rs: &RefState, t: Transition) -> Self::SystemUnderTest {
        Self::apply(s, rs, t)
    }
}

impl StorageReplicationTest {
    fn init_test(_ref_state: &RefState) -> Self {
        let _ = env_logger::try_init();

        let storage_dir = StorageDir::Temp(tempdir().unwrap());
        // let storage_dir = StorageDir::Path("/Users/jack/data/zenoh-test".into());
        info!("init_test {:?}", storage_dir.path());
        Self {
            storage_dir,
            router: Default::default(),
        }
    }

    fn apply(mut state: Self, ref_state: &RefState, transition: Transition) -> Self {
        use Transition::*;
        match transition {
            StartRouter(id) => {
                info!("{transition:?}");
                state
                    .router
                    .insert(id, Some(create_zenoh_router(id, &state)));
                // Give it a bit to connect
                std::thread::sleep(Duration::from_millis(100));
            }
            StopRouter(id) => {
                info!("{transition:?}");
                // Make sure things propagate before closing the ROUTER
                std::thread::sleep(Duration::from_millis(100));
                state.router.get_mut(&id).unwrap().take();
            }
            RestartRouter(id) => {
                info!("{transition:?}");
                let rs = create_zenoh_router(id, &state);
                state.router.get_mut(&id).unwrap().replace(rs);
            }
            UpdateKey(rid, key, val) | CreateKey(rid, key, val) => {
                state
                    .router
                    .get_mut(&rid)
                    .and_then(|d| d.as_mut())
                    .map(|d| d.sesh.put(key, val).res_sync().unwrap());
            }
        }

        let start = Instant::now();
        let mut validated = false;
        while (Instant::now() - start) < Duration::from_secs(5) {
            std::thread::sleep(Duration::from_millis(100));
            if Self::validate_state(ref_state, &state) {
                validated = true;
                break;
            }
        }
        assert!(validated);
        state
    }

    fn validate_state(ref_state: &RefState, state: &Self) -> bool {
        // If we stopped the ROUTER we can't do anything.
        for (rid, router_state) in state.router.iter().filter(|d| d.1.is_some()) {
            let Some(router_state) = router_state.as_ref() else {
                continue;
            };

            let all_router_keys = router_state
                .sesh
                .get(format!("{TEST_PREFIX}/*"))
                .res_sync()
                .unwrap()
                .into_iter()
                .collect::<Vec<_>>();

            for uuid in ref_state.resources.iter().filter(|z| z.1.is_some()) {
                info!("Ref Key: {}", uuid.0);
            }

            for samp in all_router_keys
                .iter()
                .filter_map(|r| r.sample.as_ref().ok())
            {
                info!(
                    "Router {rid} key: {} val: {:?}",
                    samp.key_expr, samp.value.payload
                );
            }

            let num_ref_resources =
                ref_state.resources.iter().filter(|z| z.1.is_some()).count();
            if all_router_keys.len() != num_ref_resources {
                error!(
                    "Unequal key count (Router {rid:?}) {} vs {}",
                    all_router_keys.len(),
                    num_ref_resources
                );
                return false;
            }

            // Check that all keys in ref_state are in state and are the same
            for (key, value) in &ref_state.resources {
                let router_val: Option<String> = router_state
                    .sesh
                    .get(key)
                    .res_sync()
                    .unwrap()
                    .into_iter()
                    .filter_map(|r| r.sample.ok())
                    .map(|s| s.value.to_string())
                    .next();

                if let Some(v) = value {
                    let Some(rval) = router_val else {
                        error!("Missing key {rid:?} {key:?}");
                        return false;
                    };

                    if *v != rval {
                        error!("Mismatched values {v:?} {rval:?}");
                        return false;
                    }
                } else if router_val.is_some() {
                    error!("Key shouldn't exist {rid:?} {key:?}");
                    return false;
                }
            }
        }
        true
    }
}

fn tcp_port(id: RouterId) -> String {
    format!("tcp/127.0.0.1:{}", 6000 + id)
}

fn start_zenohd(
    id: RouterId,
    other_zenoh_rids: &[RouterId],
    storage_dir: &Path,
) -> Child {
    info!("Starting {:?} with {:?}", id, other_zenoh_rids);
    let zenoh_install_path = std::env::var("ZENOH_INSTALL_PATH").map(PathBuf::from).ok();
    if zenoh_install_path.is_none() {
        warn!("Missing ZENOH_INSTALL_PATH, falling back to /usr/bin and /usr/lib");
    }
    let zenoh_bin_path = zenoh_install_path
        .clone()
        .unwrap_or_else(|| PathBuf::from("/usr/bin"));
    let zenoh_lib_path = zenoh_install_path.unwrap_or_else(|| PathBuf::from("/usr/lib"));

    let z_rtr_cfg = serde_json::json!(
        {
            "mode": "router",
            "listen": {
                "endpoints": [tcp_port(id)],
            },
            "connect": {
                "endpoints": other_zenoh_rids.iter().copied().map(tcp_port).collect::<Vec<_>>(),
            },
            // Disable scouting as we only want the bot to connect to predetermined endpoints.
            "scouting": {
                "multicast": {
                    "enabled": false,
                },
            },
            "adminspace": {
                "permissions": {
                    "read": true,
                    "write": true
                }
            },
            "plugins": {
                // configuration of "storage-manager" plugin:
                "storage_manager": {
                    "volumes": {
                        // configuration of a "fs" volume (the "zenoh_backend_fs" backend library will be loaded at startup)
                        "fs": {},
                    },

                    "storages": {
                        "test_fs": {
                            "key_expr": format!("{TEST_PREFIX}/*"),
                            "complete": "true",
                            "volume": {
                                "id": "fs",
                                "dir": id.to_string(),
                            },
                            "replica_config": {
                                "publication_interval": 1,
                                "propagation_delay": 200,
                                "delta": 1000,
                            },
                        }
                    },

                    // This will get configured at runtime in launch command
                    "backend_search_dirs": [zenoh_lib_path],
                },
            }
        }
    );

    let cfg_path = storage_dir.join("zenohd_config.json5");
    {
        let mut f = std::fs::File::create(&cfg_path).unwrap();
        f.write_all(z_rtr_cfg.to_string().as_bytes()).unwrap();
    }

    Command::new(zenoh_bin_path.join("zenohd"))
        .env("ZENOH_BACKEND_FS_ROOT", storage_dir)
        .args([
            "--config",
            cfg_path.to_str().unwrap(),
            "--plugin-search-dir",
            zenoh_lib_path.to_str().unwrap(),
        ])
        .spawn()
        .unwrap()
}

fn create_zenoh_router(id: RouterId, state: &StorageReplicationTest) -> TestRouter {
    let router_storage_dir = state.storage_dir.path().join(id.to_string() + "/");
    std::fs::create_dir_all(&router_storage_dir).unwrap();

    let other_zenoh_rids = state
        .router
        .iter()
        .filter(|d| d.1.is_some() && *d.0 != id)
        .map(|d| *d.0)
        .collect::<Vec<_>>();

    let _router =
        ZenohRouterProcess(start_zenohd(id, &other_zenoh_rids, &router_storage_dir));
    let client_cfg = config::client(vec![tcp_port(id).parse::<EndPoint>().unwrap()]);
    let sesh = zenoh::open(client_cfg).res_sync().unwrap();
    TestRouter { _router, sesh }
}
