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
  private roomListenerEvent: string | null = null;

  private readonly jsonHeaders = new Headers({ 'Content-Type': 'application/json' });

  isLoggedIn(): boolean {
    return this.clientId !== null;
  }

  async signin(username: string, force = false): Promise<boolean> {
    this.resetSession();

    const action = force ? 'signinExisting' : 'signin';
    const response = await fetch(`${environment.SERVER_URL}/${action}`, {
      method: 'POST',
      body: username,
    });
    if (response.status === 409) {
      return false;
    }
    if (!response.ok) {
      throw new Error(`Sign-in failed (${response.status})`);
    }
    const cid = (await response.text()).trim();

    if (!cid) {
      return false;
    }

    this.username.set(username);
    this.clientId = cid;
    this.eventSource = new EventSource(`${environment.SERVER_URL}/register/${this.clientId}`);
    this.eventSource.addEventListener('roomAdded', (rsp) => {
      const newRoom = JSON.parse(rsp.data) as Room;
      this.rooms.update((rooms) => this.mergeRooms([...rooms, newRoom]));
    });
    this.eventSource.addEventListener('roomsRemoved', (rsp) => {
      const roomIds = new Set(JSON.parse(rsp.data) as string[]);
      this.rooms.update((rooms) => rooms.filter((room) => !roomIds.has(room.id)));
    });

    const roomsResponse = await fetch(`${environment.SERVER_URL}/rooms`);
    if (!roomsResponse.ok) {
      this.resetSession();
      throw new Error(`Loading rooms failed (${roomsResponse.status})`);
    }
    const initialRooms = (await roomsResponse.json()) as Room[];
    // An SSE update can arrive while the snapshot request is in flight.
    this.rooms.update((rooms) => this.mergeRooms([...initialRooms, ...rooms]));

    return true;
  }

  async signout(): Promise<void> {
    const clientId = this.clientId;
    this.resetSession();
    if (clientId !== null) {
      const response = await fetch(`${environment.SERVER_URL}/signout`, {
        method: 'POST',
        body: clientId,
      });
      if (!response.ok) {
        throw new Error(`Sign-out failed (${response.status})`);
      }
    }
  }

  findRoom(roomId: string): Room | undefined {
    return this.rooms().find((room) => room.id === roomId);
  }

  addRoom(roomName: string): Promise<Response> {
    return fetch(`${environment.SERVER_URL}/addRoom`, {
      headers: { 'Content-Type': 'text/plain' },
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

  async joinRoom(
    roomId: string,
    roomListener: (event: MessageEvent<string>) => void,
  ): Promise<Response> {
    if (this.eventSource === null || this.clientId === null) {
      throw new Error('Not signed in');
    }
    this.roomListener = roomListener;
    this.roomListenerEvent = roomId;
    this.eventSource.addEventListener(roomId, this.roomListener);

    const response = await fetch(`${environment.SERVER_URL}/join`, {
      method: 'POST',
      headers: this.jsonHeaders,
      body: JSON.stringify({
        clientId: this.clientId,
        roomId,
      }),
    });
    if (!response.ok) {
      this.eventSource.removeEventListener(roomId, this.roomListener);
      this.roomListener = null;
      this.roomListenerEvent = null;
    }
    return response;
  }

  leaveRoom(roomId: string): Promise<Response> {
    if (this.roomListener) {
      this.eventSource?.removeEventListener(this.roomListenerEvent ?? roomId, this.roomListener);
      this.roomListener = null;
      this.roomListenerEvent = null;
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

  private resetSession(): void {
    this.eventSource?.close();
    this.eventSource = null;
    this.roomListener = null;
    this.roomListenerEvent = null;
    this.clientId = null;
    this.rooms.set([]);
    this.username.set(null);
  }

  private mergeRooms(rooms: Room[]): Room[] {
    return [...new Map(rooms.map((room) => [room.id, room])).values()].sort((a, b) =>
      a.name.localeCompare(b.name),
    );
  }
}
