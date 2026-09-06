export type MessageType = 'JOIN' | 'LEAVE' | 'MSG';

export interface Message {
  sendDate: number;
  user: string;
  type: MessageType;
  message: string;
}
