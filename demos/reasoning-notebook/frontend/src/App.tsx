import { useEffect, useState } from "react";
import { fetchHealth, type HealthResponse } from "./api";

export function App() {
  const [health, setHealth] = useState<HealthResponse | null>(null);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    fetchHealth()
      .then(setHealth)
      .catch((e) => setError(e.message));
  }, []);

  return (
    <div style={styles.container}>
      <header style={styles.header}>
        <h1 style={styles.title}>Reasoning Notebook</h1>
        <StatusBadge health={health} error={error} />
      </header>
      <main style={styles.main}>
        <p style={styles.placeholder}>
          Editor, graph view, and chat panel coming in the next phases.
        </p>
      </main>
    </div>
  );
}

function StatusBadge({
  health,
  error,
}: {
  health: HealthResponse | null;
  error: string | null;
}) {
  if (error) {
    return <span style={{ ...styles.badge, background: "#f38ba8" }}>Disconnected</span>;
  }
  if (!health) {
    return <span style={{ ...styles.badge, background: "#6c7086" }}>Connecting...</span>;
  }
  const ok = health.engine === "connected";
  return (
    <span
      style={{
        ...styles.badge,
        background: ok ? "#a6e3a1" : "#f38ba8",
        color: "#1e1e2e",
      }}
    >
      {ok ? "Connected" : "Engine error"}
    </span>
  );
}

const styles: Record<string, React.CSSProperties> = {
  container: {
    minHeight: "100vh",
    display: "flex",
    flexDirection: "column",
  },
  header: {
    display: "flex",
    alignItems: "center",
    justifyContent: "space-between",
    padding: "16px 24px",
    borderBottom: "1px solid rgba(255,255,255,0.06)",
    background: "#181825",
  },
  title: {
    fontSize: 18,
    fontWeight: 600,
    letterSpacing: -0.3,
  },
  badge: {
    fontSize: 11,
    fontWeight: 600,
    padding: "4px 12px",
    borderRadius: 6,
    letterSpacing: 0.3,
  },
  main: {
    flex: 1,
    display: "flex",
    alignItems: "center",
    justifyContent: "center",
  },
  placeholder: {
    color: "#6c7086",
    fontSize: 14,
  },
};
