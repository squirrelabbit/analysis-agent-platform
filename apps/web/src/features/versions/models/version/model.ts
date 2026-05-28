import type { ClauseVersionBuild, CleanVersionBuild, GenuinenessVersionBuild } from "../build";

export interface Version {
  id: string;
  createdAt: string;
  isActive: boolean;
  rowCount: number;
  columnCount: number;
  columns: string[];
  byteSize: number;
  cleanStatus: string;
  docGenuinenessStatus: string;
  clauseLabelStatus: string;
  originalFilename: string;
}

export interface VersionDetail {
  id: string;
  createdAt: string;
  isActive: boolean;
  rowCount: number;
  columnCount: number;
  columns: string[];
  byteSize: number;
  clean: CleanVersionBuild;
  docGenuineness: GenuinenessVersionBuild;
  clauseLabel: ClauseVersionBuild;
}
