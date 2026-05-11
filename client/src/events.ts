/**
 * On-chain event decoders for Torna.
 *
 *   The program emits structured logs via sol_log_data on insert, delete,
 *   authority change, and delegate add/remove. They show up in tx log
 *   messages as "Program data: <base64>". Each event starts with a 1-byte
 *   discriminator.
 *
 *   Indexers, Geyser plugins, and analytics services can subscribe to
 *   program logs and parse these events without polling account state.
 */
import { PublicKey } from "@solana/web3.js";

export enum EventType {
  Insert = 0x01,
  Delete = 0x02,
  AuthorityChange = 0x03,
  DelegateAdded = 0x04,
  DelegateRemoved = 0x05,
}

export interface InsertEvent {
  type: "insert";
  key: Buffer;
  value: Buffer;
  leafIdx: number;
}

export interface DeleteEvent {
  type: "delete";
  key: Buffer;
  value: Buffer;
  leafIdx: number;
}

export interface AuthorityChangeEvent {
  type: "authority_change";
  oldAuthority: PublicKey;
  newAuthority: PublicKey;
}

export interface DelegateAddedEvent {
  type: "delegate_added";
  delegate: PublicKey;
}

export interface DelegateRemovedEvent {
  type: "delegate_removed";
  delegate: PublicKey;
}

export type TornaEvent =
  | InsertEvent
  | DeleteEvent
  | AuthorityChangeEvent
  | DelegateAddedEvent
  | DelegateRemovedEvent;

/** Decode a single event payload (after the discriminator byte). Returns
 *  null if the data doesn't look like a known event. `valueSize` is the
 *  tree's value_size (read from header); needed to size insert/delete payloads. */
export function decodeEvent(data: Buffer, valueSize: number = 32): TornaEvent | null {
  if (data.length < 1) return null;
  const disc = data[0];
  switch (disc) {
    case EventType.Insert: {
      const expected = 1 + 32 + valueSize + 4;
      if (data.length < expected) return null;
      return {
        type: "insert",
        key: Buffer.from(data.subarray(1, 33)),
        value: Buffer.from(data.subarray(33, 33 + valueSize)),
        leafIdx: data.readUInt32LE(33 + valueSize),
      };
    }
    case EventType.Delete: {
      const expected = 1 + 32 + valueSize + 4;
      if (data.length < expected) return null;
      return {
        type: "delete",
        key: Buffer.from(data.subarray(1, 33)),
        value: Buffer.from(data.subarray(33, 33 + valueSize)),
        leafIdx: data.readUInt32LE(33 + valueSize),
      };
    }
    case EventType.AuthorityChange: {
      if (data.length < 1 + 32 + 32) return null;
      return {
        type: "authority_change",
        oldAuthority: new PublicKey(Buffer.from(data.subarray(1, 33))),
        newAuthority: new PublicKey(Buffer.from(data.subarray(33, 65))),
      };
    }
    case EventType.DelegateAdded: {
      if (data.length < 1 + 32) return null;
      return {
        type: "delegate_added",
        delegate: new PublicKey(Buffer.from(data.subarray(1, 33))),
      };
    }
    case EventType.DelegateRemoved: {
      if (data.length < 1 + 32) return null;
      return {
        type: "delegate_removed",
        delegate: new PublicKey(Buffer.from(data.subarray(1, 33))),
      };
    }
    default:
      return null;
  }
}

/** Scan a tx's log messages and pull out every Torna event in order.
 *  Logs are passed as the array Solana returns under meta.logMessages.
 *  Any non-Torna `Program data:` lines are ignored. */
export function eventsFromLogs(logs: string[], valueSize: number = 32): TornaEvent[] {
  const events: TornaEvent[] = [];
  for (const log of logs) {
    const m = log.match(/Program data:\s+(\S+)/);
    if (!m) continue;
    const raw = Buffer.from(m[1], "base64");
    const evt = decodeEvent(raw, valueSize);
    if (evt) events.push(evt);
  }
  return events;
}

/** Pretty-print one event in a single-line, human-readable form. */
export function formatEvent(e: TornaEvent): string {
  switch (e.type) {
    case "insert": {
      const k = e.key.length >= 4 ? e.key.readUInt32BE(e.key.length - 4) : 0;
      return `INSERT  key=${k} leaf=${e.leafIdx}`;
    }
    case "delete": {
      const k = e.key.length >= 4 ? e.key.readUInt32BE(e.key.length - 4) : 0;
      return `DELETE  key=${k} leaf=${e.leafIdx}`;
    }
    case "authority_change":
      return `AUTH    ${e.oldAuthority.toBase58().slice(0, 8)}… → ${e.newAuthority.toBase58().slice(0, 8)}…`;
    case "delegate_added":
      return `DLG+    ${e.delegate.toBase58().slice(0, 8)}…`;
    case "delegate_removed":
      return `DLG-    ${e.delegate.toBase58().slice(0, 8)}…`;
  }
}
