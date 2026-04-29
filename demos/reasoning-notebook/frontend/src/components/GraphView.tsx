import { useCallback, useEffect, useMemo, useRef, useState } from "react";
import ForceGraph2D, { type ForceGraphMethods } from "react-force-graph-2d";
import {
  consolidateOntology,
  fetchGraph,
  fetchWhy,
  resolveEntities,
  type GraphData,
  type ProofTreeData,
} from "../api";
import { ProvenanceTree } from "./ProvenanceTree";
import type { Note } from "../types";

interface GraphViewProps {
  refreshKey: number;
  notes: Note[];
  onSelectNote: (noteId: string) => void;
}

const KIND_COLORS: Record<string, string> = {
  person: "#89b4fa",
  organization: "#f9e2af",
  technology: "#a6e3a1",
  concept: "#cba6f7",
  place: "#fab387",
  event: "#f38ba8",
  role: "#94e2d5",
  object: "#f5c2e7",
  building: "#fab387",
  artwork: "#cba6f7",
  animal: "#94e2d5",
  software: "#a6e3a1",
  scene: "#f5e0dc",
  emotion: "#f38ba8",
};

const DEFAULT_COLOR = "#6c7086";

function stripImgPrefix(id: string): string {
  return id.startsWith("img:") ? id.slice(4) : id;
}

function kindColor(kind: string): string {
  const k = kind.toLowerCase();
  // Exact match
  if (KIND_COLORS[k]) return KIND_COLORS[k];
  // Check if any known kind is a substring (handles "artwork/animal", "building/artwork")
  for (const [key, color] of Object.entries(KIND_COLORS)) {
    if (k.includes(key)) return color;
  }
  return DEFAULT_COLOR;
}

// eslint-disable-next-line @typescript-eslint/no-explicit-any
type GNode = any;
// eslint-disable-next-line @typescript-eslint/no-explicit-any
type GLink = any;

export function GraphView({ refreshKey, notes, onSelectNote }: GraphViewProps) {
  const [raw, setRaw] = useState<GraphData | null>(null);
  const [hovered, setHovered] = useState<GNode | null>(null);
  const [selected, setSelected] = useState<GNode | null>(null);
  const [proofTree, setProofTree] = useState<ProofTreeData | null>(null);
  const [consolidating, setConsolidating] = useState(false);
  const [resolving, setResolving] = useState(false);
  const [consolidateMsg, setConsolidateMsg] = useState<string | null>(null);
  const containerRef = useRef<HTMLDivElement>(null);
  const fgRef = useRef<ForceGraphMethods | undefined>(undefined);
  const [dimensions, setDimensions] = useState({ width: 800, height: 600 });

  useEffect(() => {
    fetchGraph().then(setRaw).catch(() => setRaw(null));
  }, [refreshKey]);

  useEffect(() => {
    const el = containerRef.current;
    if (!el) return;
    const ro = new ResizeObserver((entries) => {
      const { width, height } = entries[0].contentRect;
      setDimensions({ width, height });
    });
    ro.observe(el);
    return () => ro.disconnect();
  }, []);

  const graphData = useMemo(() => {
    if (!raw) return { nodes: [], links: [] };

    const nameToNode = new Map<string, GNode>();
    const nameToNoteIds = new Map<string, Set<string>>();

    for (const e of raw.nodes) {
      const cleanSource = stripImgPrefix(e.source_note_id);
      if (!nameToNode.has(e.name)) {
        nameToNode.set(e.name, {
          id: e.name,
          name: e.name,
          kind: e.kind,
          description: e.description,
          source_note_id: cleanSource,
        });
        nameToNoteIds.set(e.name, new Set([cleanSource]));
      } else {
        nameToNoteIds.get(e.name)!.add(cleanSource);
      }
    }

    // Attach all source note IDs to each node
    for (const [name, node] of nameToNode) {
      node.source_note_ids = Array.from(nameToNoteIds.get(name) ?? []);
    }

    const links: GLink[] = [];
    for (const r of raw.edges) {
      if (nameToNode.has(r.subject) && nameToNode.has(r.object)) {
        links.push({
          source: r.subject,
          target: r.object,
          predicate: r.predicate,
          source_note_id: r.source_note_id,
          derived: r.derived ?? false,
        });
      }
    }

    return { nodes: Array.from(nameToNode.values()), links };
  }, [raw]);

  // Compute relationships for selected node
  const selectedRelationships = useMemo(() => {
    if (!selected) return [];
    return graphData.links.filter((l: GLink) => {
      const srcId = typeof l.source === "string" ? l.source : l.source?.id;
      const tgtId = typeof l.target === "string" ? l.target : l.target?.id;
      return srcId === selected.id || tgtId === selected.id;
    });
  }, [selected, graphData.links]);

  const noteTitle = (noteId: string) => {
    const realId = stripImgPrefix(noteId);
    const note = notes.find((n) => n.id === realId);
    return note?.title ?? realId.slice(0, 8);
  };

  const nodeCanvasObject = useCallback(
    (node: GNode, ctx: CanvasRenderingContext2D, globalScale: number) => {
      const label = node.name;
      const fontSize = 12 / globalScale;
      const color = kindColor(node.kind);
      const isHovered = hovered?.id === node.id;
      const isSelected = selected?.id === node.id;
      const radius = (isHovered || isSelected) ? 7 / globalScale : 5 / globalScale;

      ctx.beginPath();
      ctx.arc(node.x!, node.y!, radius, 0, 2 * Math.PI);
      ctx.fillStyle = color;
      ctx.fill();

      if (isSelected) {
        ctx.strokeStyle = "rgba(255,255,255,0.7)";
        ctx.lineWidth = 2 / globalScale;
        ctx.stroke();
      } else if (isHovered) {
        ctx.strokeStyle = "rgba(255,255,255,0.4)";
        ctx.lineWidth = 1.5 / globalScale;
        ctx.stroke();
      }

      ctx.font = `${fontSize}px -apple-system, system-ui, sans-serif`;
      ctx.textAlign = "center";
      ctx.textBaseline = "top";
      ctx.fillStyle = "rgba(205,214,244,0.85)";
      ctx.fillText(label, node.x!, node.y! + radius + 2 / globalScale);
    },
    [hovered, selected]
  );

  const linkCanvasObject = useCallback(
    (link: GLink, ctx: CanvasRenderingContext2D, globalScale: number) => {
      const src = link.source as GNode;
      const tgt = link.target as GNode;
      if (!src.x || !tgt.x) return;

      const srcId = typeof link.source === "string" ? link.source : link.source?.id;
      const tgtId = typeof link.target === "string" ? link.target : link.target?.id;
      const isHighlighted = selected && (srcId === selected.id || tgtId === selected.id);
      const isDerived = link.derived;

      ctx.beginPath();
      if (isDerived) {
        ctx.setLineDash([4 / globalScale, 3 / globalScale]);
      }
      ctx.moveTo(src.x, src.y!);
      ctx.lineTo(tgt.x, tgt.y!);
      ctx.strokeStyle = isHighlighted
        ? "rgba(166,227,161,0.6)"
        : isDerived
          ? "rgba(203,166,247,0.35)"
          : "rgba(108,112,134,0.35)";
      ctx.lineWidth = (isHighlighted ? 2 : 1) / globalScale;
      ctx.stroke();
      ctx.setLineDash([]);

      // Only show label when edge is highlighted (node selected)
      if (isHighlighted) {
        const midX = (src.x + tgt.x) / 2;
        const midY = (src.y! + tgt.y!) / 2;
        const fontSize = 10 / globalScale;
        ctx.font = `${fontSize}px -apple-system, system-ui, sans-serif`;
        ctx.textAlign = "center";
        ctx.textBaseline = "middle";
        ctx.fillStyle = isDerived
          ? "rgba(203,166,247,0.95)"
          : "rgba(166,227,161,0.95)";
        // Background for readability
        const textWidth = ctx.measureText(link.predicate).width;
        ctx.fillStyle = "rgba(24,24,37,0.85)";
        ctx.fillRect(
          midX - textWidth / 2 - 3 / globalScale,
          midY - fontSize / 2 - 1 / globalScale,
          textWidth + 6 / globalScale,
          fontSize + 2 / globalScale
        );
        ctx.fillStyle = isDerived
          ? "rgba(203,166,247,0.95)"
          : "rgba(166,227,161,0.95)";
        ctx.fillText(link.predicate, midX, midY);
      }
    },
    [selected]
  );

  const handleConsolidate = async () => {
    setConsolidating(true);
    setConsolidateMsg(null);
    try {
      const result = await consolidateOntology();
      if (result.status === "nothing_to_consolidate") {
        setConsolidateMsg("Nothing to consolidate");
      } else if (result.status === "done") {
        const parts = [];
        if (result.predicates_renamed) parts.push(`${result.predicates_renamed} predicates renamed`);
        if (result.entities_renamed) parts.push(`${result.entities_renamed} entities merged`);
        setConsolidateMsg(parts.length ? parts.join(", ") : "No changes needed");
        fetchGraph().then(setRaw);
      } else {
        setConsolidateMsg(`Status: ${result.status}`);
      }
      setTimeout(() => setConsolidateMsg(null), 5000);
    } catch {
      setConsolidateMsg("Consolidation failed");
      setTimeout(() => setConsolidateMsg(null), 5000);
    } finally {
      setConsolidating(false);
    }
  };

  if (!raw) {
    return (
      <div style={styles.empty}>
        <p>Loading graph...</p>
      </div>
    );
  }

  if (graphData.nodes.length === 0) {
    return (
      <div style={styles.empty}>
        <p style={{ color: "#6c7086", fontSize: 14 }}>
          No entities yet. Write some notes and extract entities to see the graph.
        </p>
      </div>
    );
  }

  return (
    <div ref={containerRef} style={styles.container}>
      <ForceGraph2D
        ref={fgRef}
        width={dimensions.width}
        height={dimensions.height}
        graphData={graphData}
        nodeCanvasObject={nodeCanvasObject}
        linkCanvasObject={linkCanvasObject}
        onNodeHover={(node) => setHovered(node as GNode | null)}
        onNodeClick={(node) => setSelected(node as GNode)}
        onBackgroundClick={() => setSelected(null)}
        backgroundColor="#1e1e2e"
        linkDirectionalArrowLength={4}
        linkDirectionalArrowRelPos={0.8}
        cooldownTicks={100}
        nodePointerAreaPaint={(node, color, ctx) => {
          const n = node as GNode;
          ctx.beginPath();
          ctx.arc(n.x!, n.y!, 8, 0, 2 * Math.PI);
          ctx.fillStyle = color;
          ctx.fill();
        }}
      />

      {/* Hover tooltip (only when no selection panel) */}
      {hovered && !selected && (
        <div style={styles.tooltip}>
          <div style={styles.tooltipKind}>{hovered.kind}</div>
          <div style={styles.tooltipName}>{hovered.name}</div>
          <div style={styles.tooltipDesc}>{hovered.description}</div>
        </div>
      )}

      {/* Node detail panel */}
      {selected && (
        <div style={styles.detailPanel}>
          <div style={styles.detailHeader}>
            <div>
              <span
                style={{
                  ...styles.detailKindBadge,
                  background: `${kindColor(selected.kind)}20`,
                  color: kindColor(selected.kind),
                }}
              >
                {selected.kind}
              </span>
            </div>
            <button style={styles.closeBtn} onClick={() => setSelected(null)}>
              &times;
            </button>
          </div>

          <h3 style={styles.detailName}>{selected.name}</h3>
          <p style={styles.detailDesc}>{selected.description}</p>

          <button
            style={styles.whyBtn}
            onClick={async () => {
              try {
                const name = selected.name.replace(/"/g, '\\"');
                const res = await fetchWhy(
                  `?entity(Id, "${name}", Kind, Desc, Source)`
                );
                if (res.proof_trees.length > 0) {
                  setProofTree(res.proof_trees[0]);
                }
              } catch {
                /* engine may not support .why on this query */
              }
            }}
          >
            Why?
          </button>

          {/* Source notes */}
          <div style={styles.detailSection}>
            <div style={styles.detailSectionTitle}>Source Notes</div>
            {(selected.source_note_ids ?? [selected.source_note_id]).map(
              (nid: string) => (
                <button
                  key={nid}
                  style={styles.noteLink}
                  onClick={() => onSelectNote(stripImgPrefix(nid))}
                >
                  {noteTitle(nid)}
                </button>
              )
            )}
          </div>

          {/* Relationships */}
          {selectedRelationships.length > 0 && (
            <div style={styles.detailSection}>
              <div style={styles.detailSectionTitle}>
                Relationships ({selectedRelationships.length})
              </div>
              {selectedRelationships.map((rel: GLink, i: number) => {
                const srcId =
                  typeof rel.source === "string"
                    ? rel.source
                    : rel.source?.id;
                const tgtId =
                  typeof rel.target === "string"
                    ? rel.target
                    : rel.target?.id;
                const isOutgoing = srcId === selected.id;
                return (
                  <div key={i} style={styles.relRow}>
                    {isOutgoing ? (
                      <>
                        <span style={styles.relPred}>{rel.predicate}</span>
                        <span style={styles.relArrow}>&rarr;</span>
                        <span
                          style={styles.relTarget}
                          onClick={() => {
                            const node = graphData.nodes.find(
                              (n: GNode) => n.id === tgtId
                            );
                            if (node) setSelected(node);
                          }}
                        >
                          {tgtId}
                        </span>
                      </>
                    ) : (
                      <>
                        <span
                          style={styles.relTarget}
                          onClick={() => {
                            const node = graphData.nodes.find(
                              (n: GNode) => n.id === srcId
                            );
                            if (node) setSelected(node);
                          }}
                        >
                          {srcId}
                        </span>
                        <span style={styles.relArrow}>&rarr;</span>
                        <span style={styles.relPred}>{rel.predicate}</span>
                      </>
                    )}
                    <span
                      style={styles.relWhyLink}
                      onClick={async () => {
                        try {
                          const s = (isOutgoing ? selected.name : srcId).replace(/"/g, '\\"');
                          const o = (isOutgoing ? tgtId : selected.name).replace(/"/g, '\\"');
                          const p = rel.predicate.replace(/"/g, '\\"');
                          const res = await fetchWhy(
                            `?relationship(Id, "${s}", "${p}", "${o}", Src)`
                          );
                          if (res.proof_trees.length > 0) {
                            setProofTree(res.proof_trees[0]);
                          }
                        } catch { /* */ }
                      }}
                    >
                      why?
                    </span>
                  </div>
                );
              })}
            </div>
          )}
        </div>
      )}

      <div style={styles.toolbar}>
        <button
          style={{
            ...styles.consolidateBtn,
            opacity: consolidating ? 0.5 : 1,
          }}
          onClick={handleConsolidate}
          disabled={consolidating}
        >
          {consolidating ? "Consolidating..." : "Consolidate Ontology"}
        </button>
        <button
          style={{
            ...styles.resolveBtn,
            opacity: resolving ? 0.5 : 1,
          }}
          onClick={async () => {
            setResolving(true);
            setConsolidateMsg(null);
            try {
              const result = await resolveEntities();
              if (result.status === "done" && result.entities_renamed) {
                setConsolidateMsg(`Merged ${result.entities_renamed} duplicate entities`);
                fetchGraph().then(setRaw);
              } else {
                setConsolidateMsg("No duplicates found");
              }
              setTimeout(() => setConsolidateMsg(null), 5000);
            } catch {
              setConsolidateMsg("Resolution failed");
              setTimeout(() => setConsolidateMsg(null), 5000);
            } finally {
              setResolving(false);
            }
          }}
          disabled={resolving}
        >
          {resolving ? "Resolving..." : "Resolve Entities"}
        </button>
        {consolidateMsg && (
          <span style={styles.consolidateMsg}>{consolidateMsg}</span>
        )}
      </div>
      <div style={styles.legend}>
        {Object.entries(KIND_COLORS).map(([kind, color]) => (
          <span key={kind} style={styles.legendItem}>
            <span style={{ ...styles.legendDot, background: color }} />
            {kind}
          </span>
        ))}
        <span style={styles.legendSep} />
        <span style={styles.legendItem}>
          <span style={{ width: 16, height: 2, background: "#a6e3a1", display: "inline-block", borderRadius: 1 }} />
          extracted
        </span>
        <span style={styles.legendItem}>
          <span style={{ width: 16, height: 2, background: "#cba6f7", display: "inline-block", borderRadius: 1, borderTop: "1px dashed #cba6f7" }} />
          derived
        </span>
      </div>

      {proofTree && (
        <ProvenanceTree tree={proofTree} onClose={() => setProofTree(null)} />
      )}
    </div>
  );
}

const styles: Record<string, React.CSSProperties> = {
  container: {
    flex: 1,
    position: "relative" as const,
    overflow: "hidden",
    background: "#1e1e2e",
    display: "flex",
  },
  empty: {
    flex: 1,
    display: "flex",
    alignItems: "center",
    justifyContent: "center",
    background: "#1e1e2e",
  },
  tooltip: {
    position: "absolute" as const,
    top: 16,
    left: 16,
    background: "rgba(24,24,37,0.95)",
    border: "1px solid rgba(255,255,255,0.08)",
    borderRadius: 8,
    padding: "10px 14px",
    maxWidth: 260,
    pointerEvents: "none" as const,
  },
  tooltipKind: {
    fontSize: 9,
    fontWeight: 600,
    textTransform: "uppercase" as const,
    letterSpacing: 1,
    color: "#89b4fa",
    marginBottom: 4,
  },
  tooltipName: {
    fontSize: 14,
    fontWeight: 600,
    color: "#cdd6f4",
    marginBottom: 4,
  },
  tooltipDesc: {
    fontSize: 12,
    color: "#a6adc8",
    lineHeight: 1.4,
  },
  // ── Detail panel ──
  detailPanel: {
    position: "absolute" as const,
    top: 0,
    right: 0,
    bottom: 0,
    width: 300,
    background: "#181825",
    borderLeft: "1px solid rgba(255,255,255,0.06)",
    padding: "16px 20px",
    overflowY: "auto" as const,
    display: "flex",
    flexDirection: "column" as const,
    gap: 12,
    zIndex: 5,
  },
  detailHeader: {
    display: "flex",
    justifyContent: "space-between",
    alignItems: "flex-start",
  },
  detailKindBadge: {
    fontSize: 10,
    fontWeight: 600,
    textTransform: "uppercase" as const,
    letterSpacing: 1,
    padding: "3px 10px",
    borderRadius: 5,
  },
  closeBtn: {
    background: "none",
    border: "none",
    color: "#6c7086",
    fontSize: 20,
    cursor: "pointer",
    padding: "0 4px",
    lineHeight: 1,
  },
  detailName: {
    fontSize: 20,
    fontWeight: 700,
    color: "#cdd6f4",
    margin: 0,
  },
  detailDesc: {
    fontSize: 13,
    color: "#a6adc8",
    lineHeight: 1.5,
    margin: 0,
  },
  whyBtn: {
    background: "rgba(203,166,247,0.12)",
    color: "#cba6f7",
    border: "none",
    borderRadius: 6,
    padding: "6px 16px",
    fontSize: 12,
    fontWeight: 600,
    cursor: "pointer",
    alignSelf: "flex-start" as const,
  },
  detailSection: {
    paddingTop: 8,
    borderTop: "1px solid rgba(255,255,255,0.06)",
    display: "flex",
    flexDirection: "column" as const,
    gap: 6,
  },
  detailSectionTitle: {
    fontSize: 10,
    fontWeight: 600,
    textTransform: "uppercase" as const,
    letterSpacing: 1,
    color: "#6c7086",
    marginBottom: 2,
  },
  noteLink: {
    background: "rgba(137,180,250,0.08)",
    border: "1px solid rgba(137,180,250,0.15)",
    borderRadius: 6,
    padding: "6px 12px",
    color: "#89b4fa",
    fontSize: 12,
    fontWeight: 500,
    cursor: "pointer",
    textAlign: "left" as const,
  },
  relRow: {
    display: "flex",
    alignItems: "center",
    gap: 6,
    fontSize: 12,
    color: "#a6adc8",
  },
  relPred: {
    color: "#a6e3a1",
    fontStyle: "italic" as const,
  },
  relArrow: {
    color: "#6c7086",
    fontSize: 11,
  },
  relTarget: {
    color: "#89b4fa",
    cursor: "pointer",
    fontWeight: 500,
  },
  relWhyLink: {
    color: "#cba6f7",
    fontSize: 10,
    cursor: "pointer",
    marginLeft: "auto",
    opacity: 0.7,
  },
  // ── Toolbar + legend ──
  toolbar: {
    position: "absolute" as const,
    top: 16,
    right: 16,
    display: "flex",
    alignItems: "center",
    gap: 10,
    background: "rgba(24,24,37,0.9)",
    border: "1px solid rgba(255,255,255,0.06)",
    borderRadius: 8,
    padding: "6px 12px",
  },
  consolidateBtn: {
    background: "rgba(249,226,175,0.12)",
    color: "#f9e2af",
    border: "none",
    borderRadius: 6,
    padding: "5px 14px",
    fontSize: 11,
    fontWeight: 600,
    cursor: "pointer",
  },
  resolveBtn: {
    background: "rgba(166,227,161,0.12)",
    color: "#a6e3a1",
    border: "none",
    borderRadius: 6,
    padding: "5px 14px",
    fontSize: 11,
    fontWeight: 600,
    cursor: "pointer",
  },
  consolidateMsg: {
    fontSize: 11,
    color: "#a6e3a1",
  },
  legend: {
    position: "absolute" as const,
    bottom: 16,
    left: 16,
    display: "flex",
    gap: 12,
    background: "rgba(24,24,37,0.9)",
    border: "1px solid rgba(255,255,255,0.06)",
    borderRadius: 8,
    padding: "8px 14px",
  },
  legendItem: {
    display: "flex",
    alignItems: "center",
    gap: 5,
    fontSize: 10,
    color: "#6c7086",
  },
  legendDot: {
    width: 6,
    height: 6,
    borderRadius: "50%",
    display: "inline-block",
  },
  legendSep: {
    width: 1,
    height: 12,
    background: "rgba(255,255,255,0.1)",
    display: "inline-block",
  },
};
