use std::net::{TcpListener, TcpStream};
use std::io::{Read, Write};
use std::sync::{Arc, Mutex};
use std::sync::mpsc::{channel, Sender, Receiver};
use std::collections::HashMap;
use std::time::{Duration, Instant};
use std::thread;
use log::{info, error};
use env_logger::Env;
use threadpool::ThreadPool;

/// Représente l'état du proxy avec les adresses des serveurs en amont,
/// les adresses des serveurs en panne, le chemin de vérification de santé,
/// l'intervalle de vérification de santé, et la gestion de la limitation de taux.
struct ProxyState {
    upstream_addresses: Vec<String>,
    dead_upstream_addresses: Arc<Mutex<Vec<String>>>,
    active_health_check_path: String,
    active_health_check_interval: Duration,
    rate_limit: Arc<RateLimit>,
}

/// Gère la limitation de taux avec un nombre maximal de requêtes par fenêtre de temps,
/// une taille de fenêtre de temps, et un compteur de requêtes pour chaque adresse IP.
struct RateLimit {
    max_requests_per_window: usize,
    rate_limit_window_size: Duration,
    request_counts: Mutex<HashMap<String, (usize, Instant)>>,
}

impl RateLimit {
    /// Crée une nouvelle instance de RateLimit.
    fn new(max_requests_per_window: usize, rate_limit_window_size: Duration) -> Self {
        RateLimit {
            max_requests_per_window,
            rate_limit_window_size,
            request_counts: Mutex::new(HashMap::new()),
        }
    }

    /// Vérifie si une IP a dépassé la limite de requêtes.
    fn check_rate_limit(&self, ip: &str) -> bool {
        let mut counts = self.request_counts.lock().unwrap();
        let (count, window_start) = counts.entry(ip.to_string()).or_insert((0, Instant::now()));
        if window_start.elapsed() > self.rate_limit_window_size {
            *count = 1;
            *window_start = Instant::now();
            true
        } else {
            if *count < self.max_requests_per_window {
                *count += 1;
                true
            } else {
                false
            }
        }
    }
}

/// Transfère les données entre deux flux TCP en utilisant des canaux pour la synchronisation.
fn transfer_data(mut reader: TcpStream, mut writer: TcpStream, tx: Sender<()>, rx: Receiver<()>) {
    let mut buffer = [0; 512];
    loop {
        let bytes_read = match reader.read(&mut buffer) {
            Ok(0) => break,  // Connexion fermée
            Ok(n) => n,      // Données lues
            Err(_) => break, // Erreur de lecture
        };

        if writer.write_all(&buffer[..bytes_read]).is_err() {
            break; // Erreur d'écriture
        }

        if rx.try_recv().is_ok() {
            break; // Arrêter le transfert si l'autre thread a terminé
        }
    }
    let _ = tx.send(()); // Notifier l'autre thread que ce thread a terminé
}

/// Gère une connexion client. Vérifie la limite de taux, puis essaie de se connecter à un serveur en amont
/// et transfère les données entre le client et le serveur en amont.
fn handle_connection(mut client_stream: TcpStream, state: Arc<ProxyState>, client_ip: String) {
    if !state.rate_limit.check_rate_limit(&client_ip) {
        error!("Rate limit exceeded for {}", client_ip);
        let response = b"HTTP/1.1 429 Too Many Requests\r\n\r\n";
        let _ = client_stream.write_all(response);
        return;
    }

    match connect_to_upstream(&state) {
        Ok(upstream_stream) => {
            let (tx1, rx1) = channel();
            let (tx2, rx2) = channel();

            let client_to_upstream = thread::spawn({
                let client_stream_clone = client_stream.try_clone().expect("Failed to clone client stream");
                let upstream_stream_clone = upstream_stream.try_clone().expect("Failed to clone upstream stream");
                move || {
                    transfer_data(client_stream_clone, upstream_stream_clone, tx1, rx2);
                }
            });

            let upstream_to_client = thread::spawn({
                move || {
                    transfer_data(upstream_stream, client_stream, tx2, rx1);
                }
            });

            client_to_upstream.join().unwrap();
            upstream_to_client.join().unwrap();
        }
        Err(_) => {
            error!("Failed to connect to any upstream servers");
            let response = b"HTTP/1.1 502 Bad Gateway\r\n\r\n";
            let _ = client_stream.write_all(response);
        }
    }
}

/// Essaie de se connecter à un serveur en amont et retourne une connexion TCP en cas de succès.
fn connect_to_upstream(state: &Arc<ProxyState>) -> Result<TcpStream, ()> {
    let mut dead_upstreams = state.dead_upstream_addresses.lock().unwrap();
    for address in &state.upstream_addresses {
        if !dead_upstreams.contains(address) {
            match TcpStream::connect(address) {
                Ok(stream) => return Ok(stream),
                Err(_) => {
                    error!("Failed to connect to upstream {}", address);
                    dead_upstreams.push(address.clone());
                }
            }
        }
    }
    Err(())
}

/// Vérifie périodiquement la santé des serveurs en amont. Envoie des requêtes de vérification de santé
/// et met à jour l'état des serveurs.
fn health_check(state: Arc<ProxyState>) {
    loop {
        {
            let mut dead_upstreams = state.dead_upstream_addresses.lock().unwrap();
            for address in &state.upstream_addresses {
                let health_check_url = format!("http://{}{}", address, state.active_health_check_path);
                match reqwest::blocking::get(&health_check_url) {
                    Ok(response) => {
                        if response.status().as_u16() == 200 {
                            dead_upstreams.retain(|a| a != address);
                            info!("{} is healthy again", address);
                        } else {
                            if !dead_upstreams.contains(address) {
                                dead_upstreams.push(address.clone());
                                error!("{} failed health check", address);
                            }
                        }
                    }
                    Err(_) => {
                        if !dead_upstreams.contains(address) {
                            dead_upstreams.push(address.clone());
                            error!("{} failed health check", address);
                        }
                    }
                }
            }
        }
        thread::sleep(state.active_health_check_interval);
    }
}

fn main() {
    env_logger::Builder::from_env(Env::default().default_filter_or("info")).init();

    let upstream_addresses = vec![
        "127.0.0.1:8081".to_string(),
        "127.0.0.1:8082".to_string(),
    ];
    let state = Arc::new(ProxyState {
        upstream_addresses,
        dead_upstream_addresses: Arc::new(Mutex::new(Vec::new())),
        active_health_check_path: "/sante_check".to_string(),
        active_health_check_interval: Duration::from_secs(10),
        rate_limit: Arc::new(RateLimit::new(100, Duration::from_secs(60))),
    });

    let listener = TcpListener::bind("127.0.0.1:8080").unwrap();
    info!("Listening on 127.0.0.1:8080");

    let state_clone = Arc::clone(&state);
    thread::spawn(move || {
        health_check(state_clone);
    });

    let pool = ThreadPool::new(4);

    for stream in listener.incoming() {
        match stream {
            Ok(stream) => {
                let state = Arc::clone(&state);
                let client_ip = stream.peer_addr().unwrap().ip().to_string();
                pool.execute(move || {
                    handle_connection(stream, state, client_ip);
                });
            }
            Err(e) => {
                error!("Failed to accept connection: {}", e);
            }
        }
    }
}
