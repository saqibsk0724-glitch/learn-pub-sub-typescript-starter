
import type { ConfirmChannel } from "amqplib";

import type {
  GameState,
  PlayingState,
} from "../internal/gamelogic/gamestate.js";

import type {
  ArmyMove,
  RecognitionOfWar,
} from "../internal/gamelogic/gamedata.js";

import { handlePause } from "../internal/gamelogic/pause.js";

import {
  handleMove,
  MoveOutcome,
} from "../internal/gamelogic/move.js";

import {
  handleWar,
  WarOutcome,
} from "../internal/gamelogic/war.js";

import { AckType } from "../internal/pubsub/consume.js";
import { publishJSON } from "../internal/pubsub/publish.js";

import {
  ExchangePerilTopic,
  WarRecognitionsPrefix,
} from "../internal/routing/routing.js";

import { publishGameLog } from "./index.js";

export function handlerPause(
  gs: GameState,
): (ps: PlayingState) => AckType {
  return (ps: PlayingState) => {
    handlePause(gs, ps);
    process.stdout.write("> ");
    return AckType.Ack;
  };
}

export function handlerMove(
  gs: GameState,
  ch: ConfirmChannel,
): (move: ArmyMove) => Promise<AckType> {
  return async (move: ArmyMove) => {
    const outcome = handleMove(gs, move);

    switch (outcome) {
      case MoveOutcome.Safe:
        process.stdout.write("> ");
        return AckType.Ack;

      case MoveOutcome.MakeWar: {
        const rw: RecognitionOfWar = {
          attacker: move.player,
          defender: gs.getPlayerSnap(),
        };

        try {
          await publishJSON<RecognitionOfWar>(
            ch,
            ExchangePerilTopic,
            `${WarRecognitionsPrefix}.${gs.getPlayerSnap().username}`,
            rw,
          );
        } catch (err) {
          console.error("Failed to publish war declaration:", err);
          process.stdout.write("> ");
          return AckType.NackRequeue;
        }

        process.stdout.write("> ");
        return AckType.Ack;
      }

      case MoveOutcome.SamePlayer:
      default:
        process.stdout.write("> ");
        return AckType.NackDiscard;
    }
  };
}
      

export function handlerWar(
  gs: GameState,
  ch: ConfirmChannel,
): (rw: RecognitionOfWar) => Promise<AckType> {
  return async (rw: RecognitionOfWar) => {
    const resolution = handleWar(gs, rw);

    let message: string;

    switch (resolution.result) {
      case WarOutcome.NotInvolved:
        process.stdout.write("> ");
        return AckType.NackRequeue;

      case WarOutcome.NoUnits:
        process.stdout.write("> ");
        return AckType.NackDiscard;

      case WarOutcome.OpponentWon:
      case WarOutcome.YouWon:
        message =
          `${resolution.winner} won a war against ${resolution.loser}`;
        break;

      case WarOutcome.Draw:
        message =
          `A war between ${resolution.attacker} and ${resolution.defender} resulted in a draw`;
        break;

      default:
        console.error("Unknown war outcome:", resolution);
        process.stdout.write("> ");
        return AckType.NackDiscard;
    }

    try {
      await publishGameLog(
        ch,
        rw.attacker.username,
        message,
      );
    } catch (err) {
      console.error("Failed to publish game log:", err);
      process.stdout.write("> ");
      return AckType.NackRequeue;
    }

    process.stdout.write("> ");
    return AckType.Ack;
  };
}
