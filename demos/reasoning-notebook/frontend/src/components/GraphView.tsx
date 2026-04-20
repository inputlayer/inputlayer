import { useCallback, useEffect, useMemo, useRef, useState } from "react";
import ForceGraph2D, { type ForceGraphMethods } from "react-force-graph-2d";
import { consolidateOntology, fetchGraph, type GraphData } from "../api";

interface GraphViewProps {
  refreshKey: number;
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
};

const DEFAULT_COLOR = "#6c7086";

// eslint-disable-next-line @typescript-eslint/no-explicit-any
type GNode = any;
// eslint-disable-next-line @typescript-eslint/no-explicit-any
type GLink = any;

export function GraphView({ refreshKey, onSelectNote }: GraphViewProps) {
  const [raw, setRaw] = useState<GraphData | null>(null);
  const [hovered, setHovered] = useState<GNode | null>(null);
  const [consolidating, setConsolidating] = useState(false);
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
    for (const e of raw.nodes) {
      if (!nameToNode.has(e.name)) {
        nameToNode.set(e.name, {
          id: e.name,
          name: e.name,
          kind: e.kind,
          description: e.description,
          source_note_id: e.source_note_id,
        });
      }
    }

    const links: GLink[] = [];
    for (const r of raw.edges) {
      if (nameToNode.has(r.subject) && nameToNode.has(r.object)) {
        links.push({
          source: r.subject,
          target: r.object,
          predicate: r.predicate,
          source_note_id: r.source_note_id,
        });
      }
    }

    return { nodes: Array.from(nameToNode.values()), links };
  }, [raw]);

  const nodeCanvasObject = useCallback(
    (node: GNode, ctx: CanvasRenderingContext2D, globalScale: number) => {
      const label = node.name;
      const fontSize = 12 / globalScale;
      const color = KIND_COLORS[node.kind] ?? DEFAULT_COLOR;
      const isHovered = hovered?.id === node.id;
      const radius = isHovered ? 7 / globalScale : 5 / globalScale;

      // Node circle
      ctx.beginPath();
      ctx.arc(node.x!, node.y!, radius, 0, 2 * Math.PI);
      ctx.fillStyle = color;
      ctx.fill();

      if (isHovered) {
        ctx.strokeStyle = "rgba(255,255,255,0.4)";
        ctx.lineWidth = 1.5 / globalScale;
        ctx.stroke();
      }

      // Label
      ctx.font = `${fontSize}px -apple-system, system-ui, sans-serif`;
      ctx.textAlign = "center";
      ctx.textBaseline = "top";
      ctx.fillStyle = "rgba(205,214,244,0.85)";
      ctx.fillText(label, node.x!, node.y! + radius + 2 / globalScale);
    },
    [hovered]
  );

  const linkCanvasObject = useCallback(
    (link: GLink, ctx: CanvasRenderingContext2D, globalScale: number) => {
      const src = link.source as GNode;
      const tgt = link.target as GNode;
      if (!src.x || !tgt.x) return;

      // Line
      ctx.beginPath();
      ctx.moveTo(src.x, src.y!);
      ctx.lineTo(tgt.x, tgt.y!);
      ctx.strokeStyle = "rgba(108,112,134,0.35)";
      ctx.lineWidth = 1 / globalScale;
      ctx.stroke();

      // Label
      const midX = (src.x + tgt.x) / 2;
      const midY = (src.y! + tgt.y!) / 2;
      const fontSize = 9 / globalScale;
      ctx.font = `${fontSize}px -apple-system, system-ui, sans-serif`;
      ctx.textAlign = "center";
      ctx.textBaseline = "middle";
      ctx.fillStyle = "rgba(166,227,161,0.6)";
      ctx.fillText(link.predicate, midX, midY);
    },
    []
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
        onNodeClick={(node) => {
          const n = node as GNode;
          if (n.source_note_id) onSelectNote(n.source_note_id);
        }}
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
      {hovered && (
        <div style={styles.tooltip}>
          <div style={styles.tooltipKind}>
            {hovered.kind}
          </div>
          <div style={styles.tooltipName}>{hovered.name}</div>
          <div style={styles.tooltipDesc}>{hovered.description}</div>
        </div>
      )}
      <div style={styles.toolbar}>
        <button
          style={{ ...styles.consolidateBtn, opacity: consolidating ? 0.5 : 1 }}
          onClick={handleConsolidate}
          disabled={consolidating}
        >
          {consolidating ? "Consolidating..." : "Consolidate Ontology"}
        </button>
        {consolidateMsg && <span style={styles.consolidateMsg}>{consolidateMsg}</span>}
      </div>
      <div style={styles.legend}>
        {Object.entries(KIND_COLORS).map(([kind, color]) => (
          <span key={kind} style={styles.legendItem}>
            <span style={{ ...styles.legendDot, background: color }} />
            {kind}
          </span>
        ))}
      </div>
    </div>
  );
}

const styles: Record<string, React.CSSProperties> = {
  container: {
    flex: 1,
    position: "relative" as const,
    overflow: "hidden",
    background: "#1e1e2e",
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
  consolidateMsg: {
    fontSize: 11,
    color: "#a6e3a1",
  },
  legend: {
    position: "absolute" as const,
    bottom: 16,
    right: 16,
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
};
