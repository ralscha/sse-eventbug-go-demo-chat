import { Service, signal } from '@angular/core';
import { Room } from '../models/room';
import { environment } from '../../environments/environment';

@Service()
export class ChatService {
  readonly rooms = signal<Room[]>([]);
  readonly username = signal<string | null>(null);

  private eventSource: EventSource | null = null;
  private clientId: string | null = null;
  private roomListener: ((event: MessageEvent<string>) => void) | null = null;

  private jsonHeaders = new Headers({ 'Content-Type': 'application/json' });

  isLoggedIn(): boolean {
    return this.clientId !== null;
  }

  async signin(username: string, force = false): Promise<boolean> {
    this.clientId = null;
    this.rooms.set([]);
    this.username.set(null);

    let url = 'signin';
    if (force) {
      url = 'signinExisting';
    }

    const response = await fetch(`${environment.SERVER_URL}/${url}`, {
      method: 'POST',
      body: username,
    });
    const cid = await response.text();

    if (!cid) {
      return false;
    }

    this.username.set(username);
    this.clientId = cid;
    this.eventSource = new EventSource(`${environment.SERVER_URL}/register/${this.clientId}`);
    this.eventSource.addEventListener('roomAdded', (rsp) => {
      const newRoom = JSON.parse(rsp.data) as Room;
      this.rooms.update((rooms) => [...rooms, newRoom]);
    });
    this.eventSource.addEventListener('roomsRemoved', (rsp) => {
      const roomIds = JSON.parse(rsp.data) as string[];
      this.rooms.update((rooms) => rooms.filter((room) => roomIds.indexOf(room.id) === -1));
    });

    const resp = await fetch(`${environment.SERVER_URL}/subscribe`, {
      method: 'POST',
      body: this.clientId,
    });

    this.rooms.set((await resp.json()) as Room[]);

    return true;
  }

  async signout(): Promise<void> {
    await fetch(`${environment.SERVER_URL}/signout`, {
      method: 'POST',
      body: this.clientId,
    });

    this.clientId = null;
    this.rooms.set([]);
    this.username.set(null);

    if (this.eventSource) {
      this.eventSource.close();
      this.eventSource = null;
    }
  }

  findRoom(roomId: string): Room | undefined {
    return this.rooms().find((room) => room.id === roomId);
  }

  addRoom(roomName: string): Promise<Response> {
    return fetch(`${environment.SERVER_URL}/addRoom`, {
      headers: this.jsonHeaders,
      method: 'POST',
      body: roomName,
    });
  }

  send(roomId: string, message: string): Promise<Response> {
    return fetch(`${environment.SERVER_URL}/send`, {
      headers: this.jsonHeaders,
      method: 'POST',
      body: JSON.stringify({
        clientId: this.clientId,
        message,
        roomId,
      }),
    });
  }

  joinRoom(roomId: string, roomListener: (event: MessageEvent<string>) => void): Promise<Response> {
    this.roomListener = roomListener;
    this.eventSource?.addEventListener(roomId, this.roomListener);

    return fetch(`${environment.SERVER_URL}/join`, {
      method: 'POST',
      headers: this.jsonHeaders,
      body: JSON.stringify({
        clientId: this.clientId,
        roomId,
      }),
    });
  }

  leaveRoom(roomId: string): Promise<Response> {
    if (this.roomListener) {
      this.eventSource?.removeEventListener(roomId, this.roomListener);
      this.roomListener = null;
    }

    return fetch(`${environment.SERVER_URL}/leave`, {
      method: 'POST',
      headers: this.jsonHeaders,
      body: JSON.stringify({
        clientId: this.clientId,
        roomId,
      }),
    });
  }
}
