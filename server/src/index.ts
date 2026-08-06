import { WebSocketServer, WebSocket } from 'ws';
import * as dotenv from 'dotenv';
import * as http from 'http';
import * as path from 'path';

// Charge les variables d'environnement depuis le fichier .env parent
// __dirname sera 'src' (ou 'dist' après build), donc on remonte de deux dossiers
dotenv.config({ path: path.resolve(__dirname, '../../.env') });

const PORT = parseInt(process.env.RELAY_PORT || '38420', 10);
const AUTH_TOKEN = process.env.AUTH_TOKEN;

if (!AUTH_TOKEN) {
    console.error('Erreur critique : AUTH_TOKEN manquant dans le fichier .env');
    process.exit(1);
}

// Map pour stocker les clients connectés (ID -> WebSocket)
const clients = new Map<string, WebSocket>();

// Fonction pour générer un ID unique au format XXX-XXX
function generateClientId(): string {
    let id: string;
    do {
        const part1 = Math.floor(100 + Math.random() * 900).toString();
        const part2 = Math.floor(100 + Math.random() * 900).toString();
        id = `${part1}-${part2}`;
    } while (clients.has(id));
    return id;
}

// Création du serveur HTTP pour pouvoir intercepter l'upgrade WebSocket
const server = http.createServer((req, res) => {
    // Ce serveur HTTP ne sert aucune page web
    res.writeHead(404);
    res.end();
});

const wss = new WebSocketServer({ noServer: true });

// Intercepte les requêtes d'upgrade (HTTP -> WebSocket) pour vérifier l'authentification
server.on('upgrade', (request, socket, head) => {
    const ip = request.socket.remoteAddress;

    try {
        // Extraction du token depuis l'URL (ex: ws://localhost:8080/?token=XYZ) 
        // ou via l'en-tête Authorization (ex: Bearer XYZ)
        const url = new URL(request.url || '', `http://${request.headers.host}`);
        let token = url.searchParams.get('token');
        
        if (!token && request.headers.authorization) {
            const parts = request.headers.authorization.split(' ');
            if (parts.length === 2 && parts[0].toLowerCase() === 'bearer') {
                token = parts[1];
            }
        }

        if (token !== AUTH_TOKEN) {
            console.warn(`[${ip}] Authentification échouée : token invalide ou manquant.`);
            socket.write('HTTP/1.1 401 Unauthorized\r\n\r\n');
            socket.destroy();
            return;
        }

        // Si l'authentification réussit, on finalise l'upgrade
        wss.handleUpgrade(request, socket, head, (ws) => {
            wss.emit('connection', ws, request);
        });
    } catch (error) {
        console.error(`[${ip}] Erreur lors de l'upgrade WebSocket:`, error);
        socket.destroy();
    }
});

// Gestion des connexions WebSocket établies
wss.on('connection', (ws: WebSocket, request: http.IncomingMessage) => {
    const ip = request.socket.remoteAddress;
    const clientId = generateClientId();

    // Enregistrement du client
    clients.set(clientId, ws);
    console.log(`[${ip}] Client connecté. ID assigné : ${clientId}. Total clients : ${clients.size}`);

    // Envoi du message de bienvenue
    ws.send(JSON.stringify({
        type: 'welcome',
        id: clientId
    }));

    // Gestion des messages reçus
    ws.on('message', (message: Buffer) => {
        try {
            const data = JSON.parse(message.toString());

            if (data.action === 'send' && data.targetId) {
                const targetWs = clients.get(data.targetId);

                if (targetWs && targetWs.readyState === WebSocket.OPEN) {
                    // Routage du message vers la cible
                    targetWs.send(JSON.stringify({
                        type: 'peer-message',
                        senderId: clientId,
                        payload: data.payload
                    }));
                    console.log(`[Routage] Message de ${clientId} vers ${data.targetId}`);
                } else {
                    // Cible introuvable ou déconnectée
                    ws.send(JSON.stringify({
                        type: 'error',
                        message: 'Le client cible est introuvable.'
                    }));
                    console.log(`[Routage] Échec : Cible ${data.targetId} introuvable pour ${clientId}`);
                }
            }
        } catch (error) {
            console.error(`[${ip}] Erreur de parsing JSON sur un message du client ${clientId}`);
        }
    });

    // Gestion de la déconnexion
    ws.on('close', () => {
        clients.delete(clientId);
        console.log(`[${ip}] Client ${clientId} déconnecté. Total clients : ${clients.size}`);
    });

    // Gestion des erreurs
    ws.on('error', (error) => {
        console.error(`[${ip}] Erreur sur le client ${clientId}:`, error);
    });
});

// Démarrage du serveur
server.listen(PORT, () => {
    console.log(`Serveur de signalisation (Gens-Relay) démarré sur le port ${PORT}`);
});
