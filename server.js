// ==========================================================
// Servidor Snipe Delay — conecta numa live do TikTok e repassa
// os presentes (gifts) recebidos em tempo real via WebSocket.
// ==========================================================

const express = require('express');
const cors = require('cors');
const http = require('http');
const { WebSocketServer } = require('ws');
const { TikTokLiveConnection, WebcastEvent, ControlEvent, SignConfig } = require('tiktok-live-connector');

// Se você tiver uma chave gratuita da Euler Stream (recomendado,
// evita bloqueios), configure a variável de ambiente EULER_API_KEY
// no Render. Sem ela, o servidor ainda funciona, mas usando o limite
// gratuito compartilhado (pode falhar com mais frequência).
if (process.env.EULER_API_KEY) {
  SignConfig.apiKey = process.env.EULER_API_KEY;
}

const app = express();
app.use(cors());
app.use(express.json());

const server = http.createServer(app);
const wss = new WebSocketServer({ server });

// ------------------------------------------------------------
// Estado global: qual conta do TikTok está vinculada agora e a
// conexão ativa com a live dela.
// ------------------------------------------------------------
let estado = {
  username: null,       // usuário do TikTok vinculado (sem @)
  status: 'desconectado', // desconectado | conectando | conectado | erro
  ultimoErro: null,
  roomId: null,
};

let tiktokConn = null;

function broadcast(payload) {
  const msg = JSON.stringify(payload);
  wss.clients.forEach((client) => {
    if (client.readyState === 1) client.send(msg);
  });
}

function enviarStatus() {
  broadcast({ type: 'status', ...estado });
}

// ------------------------------------------------------------
// Conecta numa live do TikTok pelo @usuario
// ------------------------------------------------------------
async function conectarTikTok(username) {
  // Se já tinha uma conexão anterior, encerra antes de abrir outra
  if (tiktokConn) {
    try { tiktokConn.disconnect(); } catch (e) {}
    tiktokConn = null;
  }

  estado.username = username;
  estado.status = 'conectando';
  estado.ultimoErro = null;
  enviarStatus();

  tiktokConn = new TikTokLiveConnection(username, {
    enableExtendedGiftInfo: true,
  });

  try {
    const state = await tiktokConn.connect();
    estado.status = 'conectado';
    estado.roomId = state.roomId || null;
    enviarStatus();
    console.log(`Conectado na live de @${username} (roomId ${state.roomId})`);
  } catch (err) {
    estado.status = 'erro';
    estado.ultimoErro = err?.message || 'Não foi possível conectar. Confira se o usuário está ao vivo agora.';
    enviarStatus();
    console.error('Erro ao conectar:', err?.message || err);
    return;
  }

  // -------------------- Evento de presente --------------------
  // Presentes "com combo" (giftType 1) disparam o evento a cada
  // repetição; só contamos o valor final quando o combo acaba
  // (repeatEnd === true) pra não duplicar a contagem.
  tiktokConn.on(WebcastEvent.GIFT, (data) => {
    const ehCombo = data.giftType === 1;
    if (ehCombo && !data.repeatEnd) return;

    const valorTotal = (data.diamondCount || 0) * (data.repeatCount || 1);

    broadcast({
      type: 'gift',
      uniqueId: data.user?.uniqueId,
      nickname: data.user?.nickname || data.user?.uniqueId,
      giftName: data.giftName,
      diamondCount: data.diamondCount,
      repeatCount: data.repeatCount,
      valorTotal,
    });
  });

  tiktokConn.on(ControlEvent.DISCONNECTED, () => {
    estado.status = 'desconectado';
    enviarStatus();
    console.log(`Desconectado da live de @${username}`);
  });

  tiktokConn.on(ControlEvent.STREAM_END, () => {
    estado.status = 'desconectado';
    estado.ultimoErro = 'A live foi encerrada.';
    enviarStatus();
  });
}

function desconectarTikTok() {
  if (tiktokConn) {
    try { tiktokConn.disconnect(); } catch (e) {}
    tiktokConn = null;
  }
  estado.status = 'desconectado';
  estado.username = null;
  estado.roomId = null;
  enviarStatus();
}

// ------------------------------------------------------------
// Rotas HTTP — usadas pelo site pra vincular/desvincular a conta
// ------------------------------------------------------------
app.get('/status', (req, res) => {
  res.json(estado);
});

app.post('/connect', async (req, res) => {
  const raw = (req.body?.username || '').trim();
  const username = raw.replace(/^@/, '');
  if (!username) {
    return res.status(400).json({ erro: 'Informe um usuário do TikTok.' });
  }
  res.json({ ok: true, mensagem: 'Conectando...' });
  conectarTikTok(username);
});

app.post('/disconnect', (req, res) => {
  desconectarTikTok();
  res.json({ ok: true });
});

// Novo cliente de WebSocket conectado no site: manda o status atual
wss.on('connection', (ws) => {
  ws.send(JSON.stringify({ type: 'status', ...estado }));
});

const PORT = process.env.PORT || 3000;
server.listen(PORT, () => {
  console.log(`Servidor rodando na porta ${PORT}`);
});
