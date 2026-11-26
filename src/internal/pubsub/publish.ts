
import type { ConfirmChannel } from "amqplib";
import {encode } from "@msgpack/msgpack"

export function publishJSON<T>(
  ch: ConfirmChannel,
  exchange: string,
  routingKey: string,
  value: T,
): Promise<void> {
  const content = Buffer.from(JSON.stringify(value));

  return new Promise<void>((resolve, reject) => {
    ch.publish(
      exchange,
      routingKey,
      content,
      {
        contentType: "application/json",
      },
      (err) => {
        if (err) {
          reject(err);
        } else {
          resolve();
        }
      },
    );
  });
}

export function publishMsgPack<T>(
  ch: ConfirmChannel,
  exchange: string,
  routingKey: string,
  value: T,
): Promise<void> {
  return new Promise<void>((resolve, reject) => {
    const body = Buffer.from(encode(value));

    ch.publish(
      exchange,
      routingKey,
      body,
      {
        contentType: "application/x-msgpack",
      },
      (err) => {
        if (err) {
          reject(err);
          return;
        }

        resolve();
      },
    );
  });
}
