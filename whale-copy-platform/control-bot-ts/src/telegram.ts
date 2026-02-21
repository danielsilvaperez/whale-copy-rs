export interface TelegramChat {
  id: number;
}

export interface TelegramMessage {
  message_id: number;
  date: number;
  text?: string;
  chat: TelegramChat;
}

export interface TelegramUpdate {
  update_id: number;
  message?: TelegramMessage;
}

interface TelegramApiResponse<T> {
  ok: boolean;
  result: T;
  description?: string;
}

export class TelegramApi {
  private readonly baseUrl: string;

  constructor(private readonly token: string) {
    this.baseUrl = `https://api.telegram.org/bot${token}`;
  }

  async getUpdates(offset: number, timeoutSec: number): Promise<TelegramUpdate[]> {
    const response = await this.request<TelegramUpdate[]>("getUpdates", {
      offset,
      timeout: timeoutSec,
      allowed_updates: ["message"],
    });

    return response;
  }

  async sendMessage(chatId: string | number, text: string): Promise<void> {
    await this.request("sendMessage", {
      chat_id: chatId,
      text,
      disable_web_page_preview: true,
    });
  }

  private async request<T>(method: string, body: Record<string, unknown>): Promise<T> {
    const response = await fetch(`${this.baseUrl}/${method}`, {
      method: "POST",
      headers: {
        "content-type": "application/json",
      },
      body: JSON.stringify(body),
    });

    if (!response.ok) {
      throw new Error(`Telegram HTTP ${response.status}`);
    }

    const data = (await response.json()) as TelegramApiResponse<T>;
    if (!data.ok) {
      throw new Error(data.description || `Telegram API ${method} failed`);
    }

    return data.result;
  }
}
