use rand::Rng;
use std::collections::HashMap;
use std::sync::mpsc::{self, RecvTimeoutError};
use std::thread;
use std::time::{Duration, Instant};

const DURACION_SIMULACION: Duration = Duration::from_secs(5);

// Estados posibles de cada nodo
#[derive(Clone, Copy, PartialEq)]
pub enum Salud {
    Vivo,
    Sospechoso,
    Caido,
}

// El monitor guarda los ultimos latidos y el estado de cada nodo
struct Monitor {
    ultimos_heartbeats: HashMap<i32, Instant>,
    estados: HashMap<i32, Salud>,
    lider: i32,
}

impl Monitor {
    fn nuevo() -> Self {
        let ahora = Instant::now();

        let mut ultimos_heartbeats = HashMap::new();
        ultimos_heartbeats.insert(1, ahora);
        ultimos_heartbeats.insert(2, ahora);
        ultimos_heartbeats.insert(3, ahora);

        let mut estados = HashMap::new();
        estados.insert(1, Salud::Vivo);
        estados.insert(2, Salud::Vivo);
        estados.insert(3, Salud::Vivo);

        Self {
            ultimos_heartbeats,
            estados,
            lider: 3,
        }
    }

    // El monitor actualiza el momento en que recibe un heartbeat
    fn actualizar_heartbeat(&mut self, id: i32) {
        if self.ultimos_heartbeats.contains_key(&id) {
            self.ultimos_heartbeats.insert(id, Instant::now());
        }
    }

    // Compara el tiempo desde el ultimo heartbeat y actualiza el estado de cada nodo
    fn comprobar_estados(&mut self) -> Vec<(i32, Salud)> {
        let mut cambios = vec![];

        for id in 1..=3 {
            let ultimo = *self.ultimos_heartbeats.get(&id).unwrap();
            let demora = ultimo.elapsed();

            let nuevo = if demora >= Duration::from_secs(2) {
                Salud::Caido
            } else if demora >= Duration::from_secs(1) {
                Salud::Sospechoso
            } else {
                Salud::Vivo
            };

            let anterior = *self.estados.get(&id).unwrap();

            if anterior != nuevo {
                self.estados.insert(id, nuevo);
                cambios.push((id, nuevo));
            }
        }

        cambios
    }

    // Elige como lider el nodo disponible con el ID mas alto
    fn elegir_lider(&mut self) {
        for id in (1..=3).rev() {
            if self.estados.get(&id) != Some(&Salud::Caido) {
                self.lider = id;
                println!("Nuevo lider: {}", id);
                return;
            }
        }
    }
}

// Simulacion
pub fn simular_fallos() -> i32 {
    let (tx, rx) = mpsc::channel::<i32>();
    let mut monitor = Monitor::nuevo();
    let inicio = Instant::now();

    // Se elige aleatoriamente el nodo que se va a caer
    let mut rng = rand::thread_rng();
    let nodo_que_falla = rng.gen_range(1..=3);

    println!("Lider inicial: {}", monitor.lider);

    let mut nodos = vec![];

    for id in 1..=3 {
        let tx_nodo = tx.clone();

        let hilo = thread::spawn(move || {
            loop {
                // Los nodos vivos terminan una vez pasado el tiempo de la simulacion
                if inicio.elapsed() >= DURACION_SIMULACION {
                    break;
                }

                // El nodo elegido deja de enviar heartbeats a los 1500 ms
                if id == nodo_que_falla && inicio.elapsed() >= Duration::from_millis(1500) {
                    println!("Nodo {} se cae", id);
                    break;
                }

                // Se envia el heartbeat al monitor
                if tx_nodo.send(id).is_err() {
                    break;
                }

                thread::sleep(Duration::from_millis(500));
            }
        });

        nodos.push(hilo);
    }

    drop(tx);

    while inicio.elapsed() < DURACION_SIMULACION {
        match rx.recv_timeout(Duration::from_millis(100)) {
            Ok(id) => {
                println!("Heartbeat recibido del nodo {}", id);
                monitor.actualizar_heartbeat(id);
            }
            Err(RecvTimeoutError::Timeout) => {}
            Err(RecvTimeoutError::Disconnected) => break,
        }

        for (id, estado) in monitor.comprobar_estados() {
            let nombre = match estado {
                Salud::Vivo => "Vivo",
                Salud::Sospechoso => "Sospechoso",
                Salud::Caido => "Caido",
            };

            println!("Nodo {}: {}", id, nombre);
        }

        // Si el que se cae es el lider, se elige uno nuevo
        if monitor.estados.get(&monitor.lider) == Some(&Salud::Caido) {
            monitor.elegir_lider();
        }
    }

    for nodo in nodos {
        nodo.join().unwrap();
    }

    println!("Lider final: {}", monitor.lider);
    monitor.lider
}