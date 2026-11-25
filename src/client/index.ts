
import amqp from "amqplib";
import { clientWelcome } from "../internal/gamelogic/gamelogic.js";
import { subscribeJSON, SimpleQueueType } from "../internal/pubsub/consume.js";
import { ExchangePerilDirect, ExchangePerilTopic,  PauseKey, ArmyMovesPrefix } from "../internal/routing/routing.js";
import { GameState } from "../internal/gamelogic/gamestate.js";
import { getInput , commandStatus , printClientHelp , printQuit } from "../internal/gamelogic/gamelogic.js";
import { commandSpawn } from "../internal/gamelogic/spawn.js";
import { commandMove } from "../internal/gamelogic/move.js";
import { handlerPause , handlerMove } from "./handlers.js";
import type { PlayingState } from "../internal/gamelogic/gamestate.js";
import { publishJSON } from "../internal/pubsub/publish.js";


async function main() {
  const rabbitConnString =
    "amqp://guest:guest@localhost:5672/";

  const conn = await amqp.connect(rabbitConnString);
  const ch = await conn.createConfirmChannel();

  console.log("Successfully connected to RabbitMQ!");

  process.once("SIGINT", async () => {
    console.log("\nShutting down Peril client...");
    await conn.close();
    process.exit(0);
  });

  const username = await clientWelcome();

  

  const gameState = new GameState(username);

await subscribeJSON<PlayingState>(
  conn,
  ExchangePerilDirect,
  `${PauseKey}.${username}`,
  PauseKey,
  SimpleQueueType.Transient,
  handlerPause(gameState),
);



await subscribeJSON(
  conn,
  ExchangePerilTopic,
  `${ArmyMovesPrefix}.${username}`,
  `${ArmyMovesPrefix}.*`,
  SimpleQueueType.Transient,
  handlerMove(gameState),
);


while (true) {
  const words = await getInput();

  if (words.length === 0) {
    continue;
  }

  switch (words[0]) {
    case "spawn":
      try {
        commandSpawn(gameState , words);
       
      } catch (err) {
        if (err instanceof Error) {
          console.error(err.message);
        }
      }
      break;

    case "move":
  try {
    const move = commandMove(gameState, words);

    await publishJSON(
      ch,
      ExchangePerilTopic,
      `${ArmyMovesPrefix}.${username}`,
      move,
    );

    console.log("Move published successfully!");
  } catch (err) {
    if (err instanceof Error) {
      console.error(err.message);
    }
  }
  break;

    case "status":
      commandStatus(gameState);
      break;

    case "help":
      printClientHelp();
      break;

    case "spam":
      console.log("Spamming not allowed yet!");
      break;

    case "quit":
      printQuit();
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
