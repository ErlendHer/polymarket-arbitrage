import { describe, it, expect, beforeAll, afterAll } from "vitest";
import { LookupTable } from "../signals/lookup.js";
import path from "path";
import fs from "fs";

const DB_PATH = path.join(__dirname, "../../../data/lookup.db");

// Skip tests if lookup DB doesn't exist (CI environment)
const dbExists = fs.existsSync(DB_PATH);

describe.skipIf(!dbExists)("LookupTable", () => {
  let lookup: LookupTable;

  beforeAll(() => {
    lookup = new LookupTable(DB_PATH);
  });

  afterAll(() => {
    lookup.close();
  });

  describe("metadata", () => {
    it("should load metadata", () => {
      const meta = lookup.getMetadata();
      expect(meta.totalEntries).toBeGreaterThan(0);
      expect(meta.snapshotIntervals.length).toBeGreaterThan(0);
      expect(meta.priorAlpha).toBe(2);
      expect(meta.priorBeta).toBe(2);
    });

    it("should have expected snapshot intervals", () => {
      const intervals = lookup.getSnapshotIntervals();
      expect(intervals).toContain(5);
      expect(intervals).toContain(30);
      expect(intervals).toContain(60);
      expect(intervals).toContain(295);
    });
  });

  describe("nearestInterval", () => {
    it("should return exact match", () => {
      expect(lookup.nearestInterval(30)).toBe(30);
    });

    it("should snap to nearest", () => {
      expect(lookup.nearestInterval(33)).toBe(30);
      expect(lookup.nearestInterval(38)).toBe(45);
    });

    it("should handle edge cases", () => {
      expect(lookup.nearestInterval(0)).toBe(5);
      expect(lookup.nearestInterval(300)).toBe(295);
    });
  });

  describe("query", () => {
    it("should return result for valid query", () => {
      const result = lookup.query(30, 0.0005, -1);
      expect(result).not.toBeNull();
      expect(result!.timeElapsedS).toBe(30);
      expect(result!.probUp).toBeGreaterThan(0);
      expect(result!.probUp).toBeLessThan(1);
      expect(result!.sampleCount).toBeGreaterThan(0);
    });

    it("should show UP bias for positive price change", () => {
      const result = lookup.query(30, 0.001, -1);
      expect(result).not.toBeNull();
      // Price up -> should predict UP more likely
      expect(result!.probUp).toBeGreaterThan(0.5);
    });

    it("should show DOWN bias for negative price change", () => {
      const result = lookup.query(30, -0.001, -1);
      expect(result).not.toBeNull();
      // Price down -> should predict DOWN more likely
      expect(result!.probUp).toBeLessThan(0.5);
    });

    it("should be near 50% for zero price change", () => {
      const result = lookup.query(30, 0, -1);
      expect(result).not.toBeNull();
      expect(Math.abs(result!.probUp - 0.5)).toBeLessThan(0.05);
    });

    it("should have credible intervals", () => {
      const result = lookup.query(60, 0.0005, -1);
      expect(result).not.toBeNull();
      expect(result!.credibleInterval[0]).toBeLessThan(result!.probUp);
      expect(result!.credibleInterval[1]).toBeGreaterThan(result!.probUp);
    });
  });

  describe("computeEdge", () => {
    it("should compute positive edge when market overprices UP", () => {
      // Price is flat, market thinks 65% UP but model says ~50%
      const edge = lookup.computeEdge(30, 0.0, 0.65, -1);
      expect(edge).not.toBeNull();
      expect(edge!.edge).toBeGreaterThan(0); // market > stat = positive edge
    });

    it("should compute negative edge when market underprices UP", () => {
      // Price up a lot, market only says 55% UP, model says ~70%
      const edge = lookup.computeEdge(60, 0.005, 0.55, -1);
      expect(edge).not.toBeNull();
      expect(edge!.edge).toBeLessThan(0); // market < stat = negative edge
    });

    it("should return null for unmatchable query", () => {
      const edge = lookup.computeEdge(30, 0.5, 0.5, 99);
      expect(edge).toBeNull();
    });
  });
});
