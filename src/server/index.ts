
import amqp from "amqplib";

import { publishJSON } from "../internal/pubsub/publish.js";

import {
  ExchangePerilDirect,
  PauseKey,
  ExchangePerilTopic,
  GameLogSlug
} from "../internal/routing/routing.js";

import type { PlayingState } from "../internal/gamelogic/gamestate.js";

import {
  printServerHelp,
  getInput,
} from "../internal/gamelogic/gamelogic.js";

import { declareAndBind , SimpleQueueType } from "../internal/pubsub/consume.js";

async function main() {
  const rabbitConnString =
    "amqp://guest:guest@localhost:5672/";

  const conn = await amqp.connect(rabbitConnString);

  console.log("Successfully connected to RabbitMQ!");

  const ch = await conn.createConfirmChannel();

  await declareAndBind(
  conn,
  ExchangePerilTopic,
  GameLogSlug,
  `${GameLogSlug}.*`,
  SimpleQueueType.Durable,
);


  process.once("SIGINT", async () => {
    await conn.close();
    process.exit(0);
  });

  printServerHelp();

  while (true) {
    const words = await getInput();

    if (words.length === 0) {
      continue;
    }

    switch (words[0]) {
      case "pause":
        console.log("Sending pause message...");

        await publishJSON<PlayingState>(
          ch,
          ExchangePerilDirect,
          PauseKey,
          { isPaused: true },
        );

        break;

      case "resume":
        console.log("Sending resume message...");

        await publishJSON<PlayingState>(
          ch,
          ExchangePerilDirect,
          PauseKey,
          { isPaused: false },
        );

        break;

      case "quit":
        console.log("Exiting Peril server...");
        break;

      default:
        console.log("Unknown command:", words[0]);
    }

    if (words[0] === "quit") {
      break;
    }
  }

  await conn.close();
  process.exit(0);
}

main().catch((err) => {
  console.error("Fatal error:", err);
  process.exit(1);
});
