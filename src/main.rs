use std::collections::VecDeque;
use std::net::{IpAddr, SocketAddr};
use std::ops::Deref;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use anyhow::Error;
use async_trait::async_trait;
use chrono::Utc;
use futures::{FutureExt, SinkExt, StreamExt, TryFutureExt};
use patch_db::json_ptr::JsonPointer;
use patch_db::{DiffPatch, Dump, PatchDb, Revision};
use rpc_toolkit::hyper::http::Error as HttpError;
use rpc_toolkit::hyper::{Body, Method, Request, Response, Server, StatusCode};
use rpc_toolkit::rpc_server_helpers::{DynMiddlewareStage2, DynMiddlewareStage3};
use rpc_toolkit::serde_json::Value;
use rpc_toolkit::url::Host;
use rpc_toolkit::yajrc::RpcError;
use rpc_toolkit::{command, rpc_server, Context};
use serde::Deserialize;
use tokio::fs::File;
use tokio::io::AsyncReadExt;
use tokio::sync::RwLock;
use tokio_tungstenite::tungstenite::Message;

#[async_trait]
pub trait AsyncFileExt: Sized {
    async fn maybe_open<P: AsRef<Path> + Send + Sync>(path: P) -> std::io::Result<Option<Self>>;
    async fn delete<P: AsRef<Path> + Send + Sync>(path: P) -> std::io::Result<()>;
}
#[async_trait]
impl AsyncFileExt for File {
    async fn maybe_open<P: AsRef<Path> + Send + Sync>(path: P) -> std::io::Result<Option<Self>> {
        match File::open(path).await {
            Ok(f) => Ok(Some(f)),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(None),
            Err(e) => Err(e),
        }
    }
    async fn delete<P: AsRef<Path> + Send + Sync>(path: P) -> std::io::Result<()> {
        if let Ok(m) = tokio::fs::metadata(path.as_ref()).await {
            if m.is_dir() {
                tokio::fs::remove_dir_all(path).await
            } else {
                tokio::fs::remove_file(path).await
            }
        } else {
            Ok(())
        }
    }
}

#[derive(Debug, Default, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub struct RpcContextConfig {
    pub bind_rpc: Option<SocketAddr>,
    pub bind_ws: Option<SocketAddr>,
    pub db: Option<PathBuf>,
    pub revision_cache_size: Option<usize>,
}

pub struct RpcContextSeed {
    pub bind_rpc: SocketAddr,
    pub bind_ws: SocketAddr,
    pub db: PatchDb,
    pub revision_cache_size: usize,
    pub revision_cache: RwLock<VecDeque<Arc<Revision>>>,
}

#[derive(Clone)]
pub struct RpcContext(Arc<RpcContextSeed>);
impl RpcContext {
    pub async fn init<P: AsRef<Path>>(cfg_path: Option<P>) -> Result<Self, Error> {
        let cfg_path = cfg_path
            .as_ref()
            .map(|p| p.as_ref())
            .unwrap_or(Path::new("/etc/embassy/config.toml"));
        let base = if let Some(mut f) = File::maybe_open(cfg_path).await? {
            let mut s = String::new();
            f.read_to_string(&mut s).await?;
            toml::from_str(&s)?
        } else {
            RpcContextConfig::default()
        };
        let db_path = base
            .db
            .unwrap_or_else(|| Path::new("/mnt/embassy-os/embassy.db").to_owned());
        let db_path_exists = tokio::fs::metadata(&db_path).await.is_ok();
        let seed = Arc::new(RpcContextSeed {
            bind_rpc: base.bind_rpc.unwrap_or(([127, 0, 0, 1], 5959).into()),
            bind_ws: base.bind_ws.unwrap_or(([127, 0, 0, 1], 5960).into()),
            db: PatchDb::open(db_path).await?,
            revision_cache_size: base.revision_cache_size.unwrap_or(512),
            revision_cache: RwLock::new(VecDeque::new()),
        });
        if !db_path_exists {
            let root_ptr: JsonPointer = Default::default();
            seed.db
                .put(
                    &root_ptr,
                    &rpc_toolkit::serde_json::from_str::<Value>(include_str!("mock.json"))?,
                    None,
                )
                .await?;
        }
        Ok(Self(seed))
    }
}
impl Context for RpcContext {
    fn host(&self) -> Host<&str> {
        match self.0.bind_rpc.ip() {
            IpAddr::V4(a) => Host::Ipv4(a),
            IpAddr::V6(a) => Host::Ipv6(a),
        }
    }
    fn port(&self) -> u16 {
        self.0.bind_rpc.port()
    }
}
impl Deref for RpcContext {
    type Target = RpcContextSeed;
    fn deref(&self) -> &Self::Target {
        &*self.0
    }
}

#[derive(serde::Serialize)]
pub struct WithRevision<T> {
    pub response: T,
    pub revision: Arc<Revision>,
}

#[command(subcommands(db, echo))]
fn main_api(#[context] ctx: RpcContext) -> Result<RpcContext, RpcError> {
    Ok(ctx)
}

#[command(subcommands(revisions, dump, put, patch))]
fn db(#[context] ctx: RpcContext) -> Result<RpcContext, RpcError> {
    Ok(ctx)
}

#[derive(serde::Serialize)]
#[serde(untagged)]
enum RevisionsRes {
    Revisions(Vec<Arc<Revision>>),
    Dump(Dump),
}

#[command(rpc_only)]
async fn revisions(
    #[context] ctx: RpcContext,
    #[arg] since: u64,
) -> Result<RevisionsRes, RpcError> {
    let cache = ctx.revision_cache.read().await;
    if cache
        .front()
        .map(|rev| rev.id <= since + 1)
        .unwrap_or(false)
    {
        Ok(RevisionsRes::Revisions(
            cache
                .iter()
                .skip_while(|rev| rev.id < since + 1)
                .cloned()
                .collect(),
        ))
    } else {
        drop(cache);
        Ok(RevisionsRes::Dump(ctx.db.dump().await))
    }
}

#[command(rpc_only)]
async fn dump(#[context] ctx: RpcContext) -> Result<Dump, RpcError> {
    Ok(ctx.db.dump().await)
}

#[command(subcommands(ui))]
fn put(#[context] ctx: RpcContext) -> Result<RpcContext, RpcError> {
    Ok(ctx)
}

#[command(rpc_only)]
async fn ui(
    #[context] ctx: RpcContext,
    #[arg] pointer: JsonPointer,
    #[arg] value: Value,
) -> Result<WithRevision<()>, RpcError> {
    let ptr = "/ui".parse::<JsonPointer>()? + &pointer;
    Ok(WithRevision {
        response: (),
        revision: ctx.db.put(&ptr, &value, None).await?,
    })
}

#[command(rpc_only)]
async fn patch(
    #[context] ctx: RpcContext,
    #[arg] patch: DiffPatch,
) -> Result<WithRevision<()>, RpcError> {
    Ok(WithRevision {
        response: (),
        revision: ctx.db.apply(patch, None, None).await?,
    })
}

#[command]
fn echo(#[context] _: RpcContext, #[arg] message: String) -> Result<String, RpcError> {
    Ok(message)
}

fn status_fn(_: i32) -> StatusCode {
    StatusCode::OK
}

async fn subscribe(ctx: RpcContext, req: Request<Body>) -> Result<Response<Body>, Error> {
    let (res, ws_fut) = hyper_ws_listener::create_ws(req)?;
    if let Some(ws_fut) = ws_fut {
        tokio::task::spawn(async move {
            let (dump, mut sub) = ctx.db.dump_and_sub().await;
            let mut stream = ws_fut.await.unwrap().unwrap();
            stream.next().await;
            stream
                .send(Message::Text(
                    rpc_toolkit::serde_json::to_string(&dump).unwrap(),
                ))
                .await
                .unwrap();

            loop {
                let rev = sub.recv().await.unwrap();
                stream
                    .send(Message::Text(
                        rpc_toolkit::serde_json::to_string(&rev).unwrap(),
                    ))
                    .await
                    .unwrap();
            }
        });
    }

    Ok(res)
}

fn err_to_500(e: Error) -> Response<Body> {
    log::error!("{}", e);
    Response::builder()
        .status(StatusCode::INTERNAL_SERVER_ERROR)
        .body(Body::empty())
        .unwrap()
}

async fn cors<'a, 'b, Params: for<'de> Deserialize<'de> + 'static>(
    req: &mut Request<Body>,
) -> Result<Result<DynMiddlewareStage2<'a, 'b, Params>, Response<Body>>, HttpError> {
    if req.method() == Method::OPTIONS {
        Ok(Err(Response::builder()
            .header(
                "Access-Control-Allow-Origin",
                if let Some(origin) = req.headers().get("origin").and_then(|s| s.to_str().ok()) {
                    origin
                } else {
                    "*"
                },
            )
            .header("Access-Control-Allow-Methods", "*")
            .header("Access-Control-Allow-Headers", "*")
            .header("Access-Control-Allow-Credentials", "true")
            .body(Body::empty())?))
    } else {
        Ok(Ok(Box::new(|_req| {
            async move {
                let res: DynMiddlewareStage3 = Box::new(|res| {
                    async move {
                        res.headers_mut()
                            .insert("Access-Control-Allow-Origin", "*".parse()?);
                        Ok::<_, HttpError>(())
                    }
                    .boxed()
                });
                Ok::<_, HttpError>(Ok(res))
            }
            .boxed()
        })))
    }
}

async fn inner_main(cfg_path: Option<&str>) -> Result<(), Error> {
    let rpc_ctx = RpcContext::init(cfg_path).await?;
    let ws_ctx = rpc_ctx.clone();

    let rev_cache_ctx = rpc_ctx.clone();
    let revision_cache_task = tokio::spawn(async move {
        let mut sub = rev_cache_ctx.db.subscribe();
        loop {
            let rev = sub.recv().await.unwrap(); // TODO: handle falling behind
            let mut cache = rev_cache_ctx.revision_cache.write().await;
            cache.push_back(rev);
            if cache.len() > rev_cache_ctx.revision_cache_size {
                cache.pop_front();
            }
        }
    })
    .map_err(Error::from);

    let twiddler_ctx = rpc_ctx.clone();
    let twiddler_task = tokio::spawn(async move {
        loop {
            tokio::time::sleep(Duration::from_secs(5)).await;
            log::info!("updating /package-data/bitcoind/installed/status/main/started");
            twiddler_ctx
                .db
                .put(
                    &"/package-data/bitcoind/installed/status/main/started"
                        .parse::<JsonPointer>()
                        .unwrap(),
                    &Utc::now(),
                    None,
                )
                .await
                .unwrap();
        }
    })
    .map_err(Error::from);

    let rpc_server = rpc_server!({
        command: main_api,
        context: rpc_ctx,
        status: status_fn,
        middleware: [
            cors,
        ],
    })
    .map_err(Error::from);

    let ws_server = {
        let builder = Server::bind(&ws_ctx.bind_ws);

        let make_svc = ::rpc_toolkit::hyper::service::make_service_fn(move |_| {
            let ctx = ws_ctx.clone();
            async move {
                Ok::<_, ::rpc_toolkit::hyper::Error>(::rpc_toolkit::hyper::service::service_fn(
                    move |req| {
                        let ctx = ctx.clone();
                        async move {
                            match req.uri().path() {
                                "/db" => Ok(subscribe(ctx, req).await.unwrap_or_else(err_to_500)),
                                _ => Response::builder()
                                    .status(StatusCode::NOT_FOUND)
                                    .body(Body::empty()),
                            }
                        }
                    },
                ))
            }
        });
        builder.serve(make_svc)
    }
    .map_err(Error::from);

    tokio::try_join!(revision_cache_task, twiddler_task, rpc_server, ws_server)?;

    Ok(())
}

fn main() {
    let matches = clap::App::new("embassyd")
        .arg(
            clap::Arg::with_name("config")
                .short("c")
                .long("config")
                .takes_value(true),
        )
        .arg(
            clap::Arg::with_name("verbosity")
                .short("v")
                .multiple(true)
                .takes_value(false),
        )
        .get_matches();

    simple_logging::log_to_stderr(match matches.occurrences_of("verbosity") {
        0 => log::LevelFilter::Off,
        1 => log::LevelFilter::Error,
        2 => log::LevelFilter::Warn,
        3 => log::LevelFilter::Info,
        4 => log::LevelFilter::Debug,
        _ => log::LevelFilter::Trace,
    });
    let cfg_path = matches.value_of("config");
    let rt = tokio::runtime::Runtime::new().expect("failed to initialize runtime");
    match rt.block_on(inner_main(cfg_path)) {
        Ok(_) => (),
        Err(e) => {
            drop(rt);
            eprintln!("{}", e);
            log::debug!("{:?}", e);
            drop(e);
            std::process::exit(1)
        }
    }
}
