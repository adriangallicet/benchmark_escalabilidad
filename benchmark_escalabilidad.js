/**
 * benchmark_escalabilidad.js
 * ------------------------------------------------------------
 * Mide el comportamiento del broker MQTT (EMQX) ante un número
 * creciente de conexiones concurrentes, sin necesitar hardware
 * físico adicional. Para cada nivel de carga (N) mide:
 *
 *   1. Tasa de éxito de conexión y tiempo de conexión.
 *   2. Latencia de t_ack (comando -> eco) del dispositivo real,
 *      con las N conexiones fantasma ya activas de fondo.
 *   3. Tiempo de reconexión masiva: se desconectan las N
 *      conexiones de golpe y se mide cuánto tardan en reconectar
 *      (evidencia empírica de la "tormenta de reconexiones").
 *
 * Uso:
 *   1. Configurar las variables de entorno (ver abajo).
 *   2. Ejecutar:  node benchmark_escalabilidad.js
 *   3. Resultados en consola y en escalabilidad_resultados_<ts>.csv
 *
 * Requiere: npm install mqtt   (si no está ya instalado)
 * ------------------------------------------------------------
 */

const mqtt = require('mqtt');
const fs = require('fs');

// ============================================================
// CONFIG - ajustar antes de correr
// ============================================================
const CONFIG = {
  brokerUrl: process.env.MQTT_URL || 'mqtt://localhost:1883',
  username: process.env.MQTT_USERNAME || 'benchmark_escalabilidad',
  password: process.env.MQTT_PASSWORD || 'benchmark_escalabilidad',

  // Dispositivo real, para medir t_ack bajo carga (mismo esquema que rtt_benchmarkv2.js)
  userId: process.env.TEST_USER_ID || 'REEMPLAZAR_userId',
  deviceId: process.env.TEST_DEVICE_ID || 'REEMPLAZAR_deviceId',
  actuatorId: process.env.TEST_ACTUATOR_ID || 'REEMPLAZAR_actuatorId',

  // Niveles de carga a probar (cantidad de conexiones fantasma concurrentes)
  niveles: (process.env.TEST_NIVELES || '5,10,20,50').split(',').map(Number),

  // Cuántas mediciones de t_ack tomar en cada nivel de carga
  medicionesPorNivel: parseInt(process.env.TEST_MEDICIONES_POR_NIVEL || '20', 10),

  timeoutConexionMs: parseInt(process.env.TEST_TIMEOUT_CONEXION_MS || '10000', 10),
  timeoutAckMs: parseInt(process.env.TEST_TIMEOUT_ACK_MS || '10000', 10),

  delayEntreMedicionesMs: parseInt(process.env.TEST_DELAY_MS || '500', 10),
};

function buildTopics(cfg) {
  const base = `${cfg.userId}/${cfg.deviceId}/${cfg.actuatorId}`;
  return {
    cmdTopic: `${base}/actdata`,
    stateTopic: `${base}/sdata`,
  };
}

function mean(arr) {
  return arr.length ? arr.reduce((a, b) => a + b, 0) / arr.length : NaN;
}

function percentile(sortedArr, p) {
  if (!sortedArr.length) return NaN;
  const idx = Math.ceil((p / 100) * sortedArr.length) - 1;
  return sortedArr[Math.min(Math.max(idx, 0), sortedArr.length - 1)];
}

// ------------------------------------------------------------
// Paso 1: conectar N clientes fantasma, midiendo éxito y tiempo
// ------------------------------------------------------------
function conectarFlota(n, etiqueta) {
  return new Promise((resolve) => {
    const clientes = [];
    const tiemposConexion = [];
    let conectados = 0;
    let resueltos = 0;

    for (let i = 0; i < n; i++) {
      const t0 = Date.now();
      const client = mqtt.connect(CONFIG.brokerUrl, {
        clean: true,
        clientId: `fantasma_${etiqueta}_${i}_${Date.now()}`,
        username: CONFIG.username,
        password: CONFIG.password,
        connectTimeout: CONFIG.timeoutConexionMs,
        reconnectPeriod: 0, // no reconectar solo automáticamente en este paso
      });

      const timer = setTimeout(() => {
        resueltos++;
        if (resueltos === n) resolve({ clientes, tiemposConexion, conectados });
      }, CONFIG.timeoutConexionMs);

      client.on('connect', () => {
        clearTimeout(timer);
        conectados++;
        tiemposConexion.push(Date.now() - t0);
        resueltos++;
        if (resueltos === n) resolve({ clientes, tiemposConexion, conectados });
      });

      client.on('error', () => {
        /* contabilizado como no conectado si nunca dispara 'connect' */
      });

      clientes.push(client);
    }
  });
}

// ------------------------------------------------------------
// Paso 2: medir t_ack del dispositivo real, N veces, con la
// flota fantasma ya conectada de fondo
// ------------------------------------------------------------
async function medirTackBajoCarga(n) {
  const { cmdTopic, stateTopic } = buildTopics(CONFIG);

  const clienteReal = mqtt.connect(CONFIG.brokerUrl, {
    clean: true,
    clientId: 'medidor_tack_' + Date.now(),
    username: CONFIG.username,
    password: CONFIG.password,
  });

  await new Promise((resolve, reject) => {
    clienteReal.on('connect', resolve);
    clienteReal.on('error', reject);
  });
  clienteReal.subscribe(stateTopic, { qos: 0 });

  const muestras = [];
  let valorActual = true;

  for (let i = 0; i < CONFIG.medicionesPorNivel; i++) {
    const valorEsperado = valorActual;
    const t0 = Date.now();

    const rtt = await new Promise((resolve) => {
      let resuelto = false;
      const onMessage = (topic, message) => {
        if (topic !== stateTopic) return;
        try {
          const parsed = JSON.parse(message.toString());
          if (parsed.value === valorEsperado && !resuelto) {
            resuelto = true;
            clienteReal.removeListener('message', onMessage);
            clearTimeout(timer);
            resolve(Date.now() - t0);
          }
        } catch (e) { /* ignorar mensajes no parseables */ }
      };
      const timer = setTimeout(() => {
        if (!resuelto) {
          resuelto = true;
          clienteReal.removeListener('message', onMessage);
          resolve(null);
        }
      }, CONFIG.timeoutAckMs);

      clienteReal.on('message', onMessage);
      clienteReal.publish(cmdTopic, JSON.stringify({ value: valorEsperado }));
    });

    muestras.push(rtt);
    valorActual = !valorActual;
    await new Promise((r) => setTimeout(r, CONFIG.delayEntreMedicionesMs));
  }

  clienteReal.end();

  const ok = muestras.filter((v) => v !== null);
  const timeouts = muestras.length - ok.length;
  const ordenados = [...ok].sort((a, b) => a - b);

  return {
    n,
    muestras: muestras.length,
    timeouts,
    tasaPerdida: (timeouts / muestras.length) * 100,
    media: mean(ok),
    p50: percentile(ordenados, 50),
    p95: percentile(ordenados, 95),
  };
}

// ------------------------------------------------------------
// Paso 3: desconectar toda la flota de golpe y medir cuánto
// tarda en reconectar (tormenta de reconexiones)
// ------------------------------------------------------------
function medirReconexionMasiva(clientes) {
  return new Promise((resolve) => {
    const n = clientes.length;
    let reconectados = 0;
    const t0 = Date.now();

    clientes.forEach((client) => {
      client.options.reconnectPeriod = 1000; // reactivar reconexión automática para este paso
      client.once('connect', () => {
        reconectados++;
        if (reconectados === n) {
          resolve(Date.now() - t0);
        }
      });
    });

    // Timeout de seguridad: si no reconectan todos en 30s, resolvemos igual
    const timeoutSeguridad = setTimeout(() => resolve(null), 30000);

    // Forzar la desconexión simultánea (simula caída de red/broker)
    clientes.forEach((client) => {
      if (client.connected) {
        client.stream.destroy(); // corta la conexión TCP de forma abrupta
      }
    });

    // Si ya resolvió por completo, limpiar el timeout de seguridad
    const check = setInterval(() => {
      if (reconectados === n) {
        clearTimeout(timeoutSeguridad);
        clearInterval(check);
      }
    }, 200);
  });
}

function desconectarFlota(clientes) {
  clientes.forEach((c) => {
    c.reconnectPeriod = 0;
    c.end(true);
  });
}

// ============================================================
// MAIN
// ============================================================
async function main() {
  if (CONFIG.userId.startsWith('REEMPLAZAR') || CONFIG.deviceId.startsWith('REEMPLAZAR') || CONFIG.actuatorId.startsWith('REEMPLAZAR')) {
    console.error('ERROR: Falta configurar userId / deviceId / actuatorId (ver CONFIG o variables de entorno TEST_USER_ID, TEST_DEVICE_ID, TEST_ACTUATOR_ID).');
    process.exit(1);
  }

  console.log('=== Benchmark de escalabilidad (conexiones concurrentes al broker) ===');
  console.log('Broker:  ', CONFIG.brokerUrl);
  console.log('Niveles: ', CONFIG.niveles.join(', '));
  console.log('---------------------------------------------\n');

  const resultados = [];

  for (const n of CONFIG.niveles) {
    console.log(`--- Nivel N=${n} ---`);

    // Paso 1: conectar flota fantasma
    const { clientes, tiemposConexion, conectados } = await conectarFlota(n, n);
    const tasaExitoConexion = (conectados / n) * 100;
    const tiempoConexionMedio = mean(tiemposConexion);
    console.log(`Conexión: ${conectados}/${n} exitosas (${tasaExitoConexion.toFixed(1)}%), tiempo medio ${tiempoConexionMedio.toFixed(0)} ms`);

    // Paso 2: medir t_ack del dispositivo real bajo esta carga
    const tack = await medirTackBajoCarga(n);
    console.log(`t_ack bajo carga: media ${tack.media?.toFixed(1) ?? 'N/A'} ms, P95 ${tack.p95 ?? 'N/A'} ms, pérdida ${tack.tasaPerdida.toFixed(1)}%`);

    // Paso 3: reconexión masiva
    console.log('Forzando desconexión masiva para medir tiempo de reconexión...');
    const tiempoReconexion = await medirReconexionMasiva(clientes);
    console.log(`Tiempo de reconexión masiva (${n} clientes): ${tiempoReconexion !== null ? tiempoReconexion + ' ms' : 'no completó en 30s'}`);

    resultados.push({
      n,
      conectados,
      tasaExitoConexion,
      tiempoConexionMedioMs: tiempoConexionMedio,
      tackMediaMs: tack.media,
      tackP95Ms: tack.p95,
      tackTasaPerdida: tack.tasaPerdida,
      tiempoReconexionMasivaMs: tiempoReconexion,
    });

    // Limpiar antes del siguiente nivel
    desconectarFlota(clientes);
    await new Promise((r) => setTimeout(r, 2000));
    console.log('');
  }

  console.log('\n=== Resumen ===');
  const encabezados = ['N', 'Conectados', 'Éxito %', 'T.conexión (ms)', 't_ack media (ms)', 't_ack P95 (ms)', 't_ack pérdida %', 'T.reconexión (ms)'];
  const filas = resultados.map((r) => [
    r.n,
    r.conectados,
    r.tasaExitoConexion.toFixed(1),
    r.tiempoConexionMedioMs?.toFixed(0) ?? '-',
    r.tackMediaMs?.toFixed(1) ?? '-',
    r.tackP95Ms ?? '-',
    r.tackTasaPerdida.toFixed(1),
    r.tiempoReconexionMasivaMs ?? 'no completó',
  ]);
  const anchos = encabezados.map((h, i) => Math.max(h.length, ...filas.map((f) => String(f[i]).length)));
  const formatearFila = (fila) => fila.map((v, i) => String(v).padEnd(anchos[i])).join(' | ');
  console.log(formatearFila(encabezados));
  console.log(anchos.map((a) => '-'.repeat(a)).join('-|-'));
  filas.forEach((f) => console.log(formatearFila(f)));

  const ts = new Date().toISOString().replace(/[:.]/g, '-');
  const filename = `escalabilidad_resultados_${ts}.csv`;
  const header = 'n;conectados;tasa_exito_conexion_pct;tiempo_conexion_medio_ms;tack_media_ms;tack_p95_ms;tack_tasa_perdida_pct;tiempo_reconexion_masiva_ms\n';
  const rows = resultados.map((r) =>
    [r.n, r.conectados, r.tasaExitoConexion.toFixed(1), r.tiempoConexionMedioMs?.toFixed(0) ?? 'N/A', r.tackMediaMs?.toFixed(1) ?? 'N/A', r.tackP95Ms ?? 'N/A', r.tackTasaPerdida.toFixed(1), r.tiempoReconexionMasivaMs ?? 'N/A'].join(';')
  ).join('\n');
  fs.writeFileSync(filename, header + rows);
  console.log(`\nCSV guardado en: ${filename}`);

  process.exit(0);
}

main().catch((err) => {
  console.error('Error ejecutando el benchmark:', err);
  process.exit(1);
});
