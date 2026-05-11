/**
 * Local state — minimal. Tree header is now a PDA derived from (programId,
 * treeId), so we only persist the treeId and let everything else be derived.
 */
import * as fs from "fs";
import * as path from "path";
import { PublicKey } from "@solana/web3.js";

export interface PersistedState {
  programId: string;
  treeId: number;
}

export class TornaStateStore {
  programId!: PublicKey;
  treeId!: number;
  private file: string;

  constructor(file: string) {
    this.file = file;
  }

  exists(): boolean {
    return fs.existsSync(this.file);
  }

  load(): void {
    const raw = JSON.parse(fs.readFileSync(this.file, "utf8")) as PersistedState;
    this.programId = new PublicKey(raw.programId);
    this.treeId = raw.treeId;
  }

  save(): void {
    const obj: PersistedState = {
      programId: this.programId.toBase58(),
      treeId: this.treeId,
    };
    fs.mkdirSync(path.dirname(this.file), { recursive: true });
    fs.writeFileSync(this.file, JSON.stringify(obj, null, 2));
  }
}
