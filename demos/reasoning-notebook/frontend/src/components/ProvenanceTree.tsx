import { useState } from "react";
import type { ProofNode, ProofTreeData } from "../api";

interface ProvenanceTreeProps {
  tree: ProofTreeData;
  onClose: () => void;
}

export function ProvenanceTree({ tree, onClose }: ProvenanceTreeProps) {
  return (
    <div style={styles.overlay} onClick={onClose}>
      <div style={styles.panel} onClick={(e) => e.stopPropagation()}>
        <div style={styles.header}>
          <span style={styles.title}>Provenance</span>
          {tree.query && (
            <code style={styles.query}>{tree.query}</code>
          )}
          <button style={styles.closeBtn} onClick={onClose}>
            &times;
          </button>
        </div>
        <div style={styles.body}>
          {tree.roots.map((rootId) => (
            <TreeNode
              key={rootId}
              nodeId={rootId}
              nodes={tree.nodes}
              depth={0}
            />
          ))}
          {tree.roots.length === 0 && (
            <p style={styles.empty}>No proof tree available for this fact.</p>
          )}
        </div>
      </div>
    </div>
  );
}

function TreeNode({
  nodeId,
  nodes,
  depth,
}: {
  nodeId: string;
  nodes: Record<string, ProofNode>;
  depth: number;
}) {
  const node = nodes[nodeId];
  const [expanded, setExpanded] = useState(depth < 3);

  if (!node) {
    return (
      <div style={{ ...styles.node, marginLeft: depth * 20 }}>
        <span style={styles.nodeUnknown}>Unknown node: {nodeId}</span>
      </div>
    );
  }

  const hasChildren = node.children.length > 0;
  const conclusion = `${node.conclusion.pred}(${node.conclusion.args.join(", ")})`;

  const kindColor =
    node.kind === "base_fact"
      ? "#a6e3a1"
      : node.kind === "rule_application"
        ? "#89b4fa"
        : node.kind === "aggregate"
          ? "#f9e2af"
          : "#cba6f7";

  return (
    <div style={{ marginLeft: depth * 20 }}>
      <div
        style={styles.node}
        onClick={() => hasChildren && setExpanded(!expanded)}
      >
        {hasChildren && (
          <span style={styles.toggle}>{expanded ? "▾" : "▸"}</span>
        )}
        {!hasChildren && <span style={styles.toggleSpacer} />}
        <span style={{ ...styles.kindBadge, color: kindColor, borderColor: `${kindColor}33` }}>
          {node.kind.replace("_", " ")}
        </span>
        <code style={styles.conclusion}>{conclusion}</code>
      </div>

      {node.rule_id && (
        <div style={{ ...styles.meta, marginLeft: depth * 20 + 28 }}>
          rule: <code style={styles.ruleId}>{node.rule_id}</code>
        </div>
      )}

      {node.bindings && Object.keys(node.bindings).length > 0 && (
        <div style={{ ...styles.meta, marginLeft: depth * 20 + 28 }}>
          {Object.entries(node.bindings).map(([k, v]) => (
            <span key={k} style={styles.binding}>
              {k}={String(v)}
            </span>
          ))}
        </div>
      )}

      {node.source && (
        <div style={{ ...styles.meta, marginLeft: depth * 20 + 28 }}>
          source: <span style={styles.source}>{node.source}</span>
        </div>
      )}

      {expanded &&
        node.children.map((childId) => (
          <TreeNode
            key={childId}
            nodeId={childId}
            nodes={nodes}
            depth={depth + 1}
          />
        ))}
    </div>
  );
}

const styles: Record<string, React.CSSProperties> = {
  overlay: {
    position: "fixed" as const,
    inset: 0,
    background: "rgba(0,0,0,0.5)",
    display: "flex",
    alignItems: "center",
    justifyContent: "center",
    zIndex: 1000,
  },
  panel: {
    background: "#1e1e2e",
    border: "1px solid rgba(255,255,255,0.08)",
    borderRadius: 12,
    width: "90%",
    maxWidth: 700,
    maxHeight: "80vh",
    display: "flex",
    flexDirection: "column" as const,
    overflow: "hidden",
  },
  header: {
    display: "flex",
    alignItems: "center",
    gap: 12,
    padding: "14px 20px",
    borderBottom: "1px solid rgba(255,255,255,0.06)",
    background: "#181825",
  },
  title: {
    fontSize: 13,
    fontWeight: 600,
    color: "#cdd6f4",
  },
  query: {
    fontSize: 11,
    color: "#6c7086",
    background: "rgba(255,255,255,0.04)",
    padding: "2px 8px",
    borderRadius: 4,
    flex: 1,
    overflow: "hidden" as const,
    textOverflow: "ellipsis" as const,
    whiteSpace: "nowrap" as const,
  },
  closeBtn: {
    background: "none",
    border: "none",
    color: "#6c7086",
    fontSize: 20,
    cursor: "pointer",
    padding: "0 4px",
    lineHeight: 1,
    flexShrink: 0,
  },
  body: {
    padding: "16px 20px",
    overflowY: "auto" as const,
    flex: 1,
  },
  empty: {
    color: "#6c7086",
    fontSize: 13,
  },
  node: {
    display: "flex",
    alignItems: "center",
    gap: 8,
    padding: "4px 0",
    cursor: "default",
  },
  toggle: {
    fontSize: 11,
    color: "#6c7086",
    width: 14,
    cursor: "pointer",
    userSelect: "none" as const,
    flexShrink: 0,
  },
  toggleSpacer: {
    width: 14,
    flexShrink: 0,
  },
  kindBadge: {
    fontSize: 9,
    fontWeight: 600,
    textTransform: "uppercase" as const,
    letterSpacing: 0.5,
    padding: "2px 7px",
    borderRadius: 4,
    border: "1px solid",
    flexShrink: 0,
  },
  conclusion: {
    fontSize: 12,
    color: "#cdd6f4",
    fontFamily: "monospace",
  },
  meta: {
    fontSize: 10,
    color: "#6c7086",
    display: "flex",
    gap: 8,
    flexWrap: "wrap" as const,
    padding: "1px 0",
  },
  ruleId: {
    color: "#89b4fa",
    fontSize: 10,
  },
  binding: {
    background: "rgba(255,255,255,0.04)",
    padding: "1px 6px",
    borderRadius: 3,
    fontSize: 10,
    color: "#a6adc8",
  },
  source: {
    color: "#a6e3a1",
  },
  nodeUnknown: {
    color: "#f38ba8",
    fontSize: 12,
  },
};
