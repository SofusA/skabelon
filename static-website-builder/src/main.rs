use std::{
    convert::Infallible,
    fs,
    net::SocketAddr,
    path::{Path, PathBuf},
};

use anyhow::{Context, Result, bail};
use axum::{
    Router,
    extract::State,
    response::sse::{Event, KeepAlive, Sse},
    routing::get,
};
use clap::{Args, Parser, Subcommand};
use serde_json::{Value, json};
use skabelon::Templates;
use tokio::sync::broadcast;
use tokio_stream::{StreamExt, wrappers::BroadcastStream};
use tower_http::services::ServeDir;
use walkdir::WalkDir;

const RELOAD_SCRIPT: &str = r#"
<script>
(() => {
    const events = new EventSource("/__skabelon/events");

    events.addEventListener("reload", () => {
        window.location.reload();
    });
})();
</script>
"#;

#[derive(Parser)]
#[command(version, about)]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    Build(BuildArgs),
    Watch(WatchArgs),
}

#[derive(Debug, Clone, Args)]
struct Paths {
    #[arg(long, default_value = "pages")]
    pages_dir: PathBuf,

    #[arg(long, default_value = "components")]
    components_dir: PathBuf,

    #[arg(long, default_value = "assets")]
    assets_dir: PathBuf,

    #[arg(long, default_value = "build")]
    build_dir: PathBuf,

    #[arg(long, default_value = "site.json")]
    values_file: PathBuf,
}

#[derive(Debug, Args)]
struct BuildArgs {
    #[command(flatten)]
    paths: Paths,
}

#[derive(Debug, Args)]
struct WatchArgs {
    #[command(flatten)]
    paths: Paths,

    #[arg(long, default_value = "127.0.0.1:3000")]
    address: SocketAddr,
}

#[derive(Clone)]
struct AppState {
    reload_tx: broadcast::Sender<()>,
}

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();

    match cli.command {
        Command::Build(args) => build_site(&args.paths, false),
        Command::Watch(args) => watch(args).await,
    }
}

fn validate_paths(paths: &Paths) -> Result<()> {
    if !paths.pages_dir.is_dir() {
        bail!(
            "pages directory does not exist: {}",
            paths.pages_dir.display()
        );
    }

    Ok(())
}

fn load_values(path: &Path) -> Result<Value> {
    if !path.exists() {
        return Ok(json!({}));
    }

    let content =
        fs::read_to_string(path).with_context(|| format!("failed to read {}", path.display()))?;

    serde_json::from_str(&content).with_context(|| format!("failed to parse {}", path.display()))
}

fn html_glob(directory: &Path) -> String {
    format!("{}/**/*.html", directory.to_string_lossy())
}

fn load_templates(paths: &Paths) -> Result<Templates> {
    let mut templates = Templates::new();

    templates.load_glob(&html_glob(&paths.pages_dir));

    if paths.components_dir.exists() {
        templates.load_glob(&html_glob(&paths.components_dir));
    }

    Ok(templates)
}

fn copy_dir_all(src: &Path, dst: &Path) -> Result<()> {
    if !src.exists() {
        return Ok(());
    }

    for entry in WalkDir::new(src) {
        let entry = entry?;

        let relative = entry.path().strip_prefix(src)?;
        let target = dst.join(relative);

        if entry.file_type().is_dir() {
            fs::create_dir_all(&target)?;
        } else {
            if let Some(parent) = target.parent() {
                fs::create_dir_all(parent)?;
            }

            fs::copy(entry.path(), &target)?;
        }
    }

    Ok(())
}

fn build_site(paths: &Paths, inject_reload: bool) -> Result<()> {
    validate_paths(paths)?;

    let values = load_values(&paths.values_file)?;
    let templates = load_templates(paths)?;

    if paths.build_dir.exists() {
        fs::remove_dir_all(&paths.build_dir)?;
    }

    fs::create_dir_all(&paths.build_dir)?;

    copy_dir_all(&paths.assets_dir, &paths.build_dir.join("assets"))?;

    for entry in WalkDir::new(&paths.pages_dir) {
        let entry = entry?;

        if !entry.file_type().is_file() {
            continue;
        }

        if !is_html(entry.path()) {
            continue;
        }

        let template_key = entry
            .path()
            .strip_prefix(&paths.pages_dir)?
            .to_string_lossy()
            .replace('\\', "/");

        let mut rendered = templates.render(&template_key, &values);

        if inject_reload {
            rendered = inject_reload_script(rendered);
        }

        let destination = output_path(entry.path(), &paths.pages_dir, &paths.build_dir)?;

        if let Some(parent) = destination.parent() {
            fs::create_dir_all(parent)?;
        }

        fs::write(&destination, rendered)?;
    }

    Ok(())
}

fn is_html(path: &Path) -> bool {
    path.extension()
        .and_then(|x| x.to_str())
        .is_some_and(|x| x.eq_ignore_ascii_case("html"))
}

fn output_path(page: &Path, pages_dir: &Path, build_dir: &Path) -> Result<PathBuf> {
    let relative = page.strip_prefix(pages_dir)?;

    let stem = relative
        .file_stem()
        .and_then(|x| x.to_str())
        .context("invalid filename")?;

    if stem == "index" {
        let parent = relative.parent().unwrap_or_else(|| Path::new(""));

        Ok(build_dir.join(parent).join("index.html"))
    } else {
        Ok(build_dir
            .join(relative.with_extension(""))
            .join("index.html"))
    }
}

fn inject_reload_script(html: String) -> String {
    if let Some(index) = html.rfind("</body>") {
        let mut output = String::with_capacity(html.len() + RELOAD_SCRIPT.len());

        output.push_str(&html[..index]);
        output.push_str(RELOAD_SCRIPT);
        output.push_str(&html[index..]);

        output
    } else {
        format!("{html}{RELOAD_SCRIPT}")
    }
}

fn is_relevant_path(path: &Path, paths: &Paths) -> bool {
    let path = path.to_string_lossy();

    if path.contains(&*paths.build_dir.to_string_lossy()) {
        return false;
    }

    path.contains(&*paths.pages_dir.to_string_lossy())
        || path.contains(&*paths.components_dir.to_string_lossy())
        || path.contains(&*paths.assets_dir.to_string_lossy())
        || path.ends_with(&*paths.values_file.to_string_lossy())
}

fn start_watcher(paths: Paths, reload_tx: broadcast::Sender<()>) -> Result<()> {
    let watcher = filesentry::Watcher::new().context("failed to create file watcher")?;

    watcher.add_root(Path::new("."), true, |_| ())?;

    watcher.add_handler(move |events| {
        let should_rebuild = events.iter().any(|event| {
            let path = event.path.as_std_path();

            let relevant = is_relevant_path(path, &paths)
                && matches!(
                    event.ty,
                    filesentry::EventType::Modified
                        | filesentry::EventType::Create
                        | filesentry::EventType::Delete
                );

            if relevant {
                println!("{:?}: {}", event.ty, path.display());
            }

            relevant
        });

        if should_rebuild {
            match build_site(&paths, true) {
                Ok(()) => {
                    println!("Site rebuilt");
                    let _ = reload_tx.send(());
                }
                Err(error) => {
                    eprintln!("Build failed: {error:#}");
                }
            }
        }

        true
    });

    watcher.start();

    Ok(())
}

async fn watch(args: WatchArgs) -> Result<()> {
    build_site(&args.paths, true)?;

    let (reload_tx, _) = broadcast::channel(16);

    start_watcher(args.paths.clone(), reload_tx.clone())?;

    let app = Router::new()
        .route("/__skabelon/events", get(sse))
        .fallback_service(
            ServeDir::new(&args.paths.build_dir).append_index_html_on_directories(true),
        )
        .with_state(AppState { reload_tx });

    let listener = tokio::net::TcpListener::bind(args.address).await?;

    println!("Listening on http://{}", args.address);

    axum::serve(listener, app).await?;

    Ok(())
}

async fn sse(
    State(state): State<AppState>,
) -> Sse<impl tokio_stream::Stream<Item = Result<Event, Infallible>>> {
    let stream =
        BroadcastStream::new(state.reload_tx.subscribe()).filter_map(|message| match message {
            Ok(_) => Some(Ok(Event::default().event("reload").data("site changed"))),
            Err(_) => None,
        });

    Sse::new(stream).keep_alive(KeepAlive::default())
}
