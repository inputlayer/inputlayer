import { useRef, useState } from "react";
import { sendChat, type ChatMessage } from "../api";

export function ChatPanel() {
  const [messages, setMessages] = useState<ChatMessage[]>([]);
  const [input, setInput] = useState("");
  const [loading, setLoading] = useState(false);
  const bottomRef = useRef<HTMLDivElement>(null);

  const handleSend = async () => {
    const text = input.trim();
    if (!text || loading) return;

    const userMsg: ChatMessage = { role: "user", content: text };
    const updated = [...messages, userMsg];
    setMessages(updated);
    setInput("");
    setLoading(true);

    try {
      const reply = await sendChat(text, updated);
      setMessages((prev) => [...prev, { role: "assistant", content: reply }]);
    } catch {
      setMessages((prev) => [
        ...prev,
        { role: "assistant", content: "Failed to get a response. Is the LLM running?" },
      ]);
    } finally {
      setLoading(false);
      setTimeout(() => bottomRef.current?.scrollIntoView({ behavior: "smooth" }), 50);
    }
  };

  const handleKeyDown = (e: React.KeyboardEvent) => {
    if (e.key === "Enter" && !e.shiftKey) {
      e.preventDefault();
      handleSend();
    }
  };

  return (
    <div style={styles.container}>
      <div style={styles.header}>
        <span style={styles.title}>Chat</span>
        <span style={styles.hint}>Ask questions across your notes</span>
      </div>
      <div style={styles.messages}>
        {messages.length === 0 && (
          <div style={styles.empty}>
            <p style={styles.emptyTitle}>Ask anything about your notes</p>
            <p style={styles.emptyHint}>
              Try: "Who works at Acme Corp?" or "How are Alice and Bob connected?"
            </p>
          </div>
        )}
        {messages.map((msg, i) => (
          <div
            key={i}
            style={{
              ...styles.bubble,
              ...(msg.role === "user" ? styles.userBubble : styles.assistantBubble),
            }}
          >
            <div style={styles.bubbleRole}>
              {msg.role === "user" ? "You" : "Assistant"}
            </div>
            <div style={styles.bubbleContent}>{msg.content}</div>
          </div>
        ))}
        {loading && (
          <div style={{ ...styles.bubble, ...styles.assistantBubble }}>
            <div style={styles.bubbleRole}>Assistant</div>
            <div style={{ ...styles.bubbleContent, color: "#6c7086" }}>Thinking...</div>
          </div>
        )}
        <div ref={bottomRef} />
      </div>
      <div style={styles.inputArea}>
        <textarea
          style={styles.input}
          value={input}
          onChange={(e) => setInput(e.target.value)}
          onKeyDown={handleKeyDown}
          placeholder="Ask a question..."
          rows={1}
        />
        <button
          style={{ ...styles.sendBtn, opacity: loading || !input.trim() ? 0.4 : 1 }}
          onClick={handleSend}
          disabled={loading || !input.trim()}
        >
          Send
        </button>
      </div>
    </div>
  );
}

const styles: Record<string, React.CSSProperties> = {
  container: {
    flex: 1,
    display: "flex",
    flexDirection: "column",
    height: "100%",
    background: "#1e1e2e",
  },
  header: {
    padding: "12px 20px",
    borderBottom: "1px solid rgba(255,255,255,0.06)",
    background: "#181825",
    display: "flex",
    alignItems: "center",
    gap: 12,
  },
  title: {
    fontSize: 13,
    fontWeight: 600,
    color: "#cdd6f4",
  },
  hint: {
    fontSize: 11,
    color: "#6c7086",
  },
  messages: {
    flex: 1,
    overflowY: "auto" as const,
    padding: "16px 20px",
    display: "flex",
    flexDirection: "column" as const,
    gap: 12,
  },
  empty: {
    flex: 1,
    display: "flex",
    flexDirection: "column" as const,
    alignItems: "center",
    justifyContent: "center",
    gap: 8,
  },
  emptyTitle: {
    fontSize: 15,
    fontWeight: 500,
    color: "#a6adc8",
  },
  emptyHint: {
    fontSize: 12,
    color: "#6c7086",
    textAlign: "center" as const,
    maxWidth: 300,
  },
  bubble: {
    borderRadius: 10,
    padding: "10px 14px",
    maxWidth: "85%",
  },
  userBubble: {
    alignSelf: "flex-end" as const,
    background: "rgba(137,180,250,0.12)",
    borderBottomRightRadius: 4,
  },
  assistantBubble: {
    alignSelf: "flex-start" as const,
    background: "rgba(255,255,255,0.04)",
    borderBottomLeftRadius: 4,
  },
  bubbleRole: {
    fontSize: 10,
    fontWeight: 600,
    textTransform: "uppercase" as const,
    letterSpacing: 0.5,
    color: "#6c7086",
    marginBottom: 4,
  },
  bubbleContent: {
    fontSize: 13,
    lineHeight: 1.6,
    color: "#cdd6f4",
    whiteSpace: "pre-wrap" as const,
  },
  inputArea: {
    padding: "12px 20px",
    borderTop: "1px solid rgba(255,255,255,0.06)",
    background: "#181825",
    display: "flex",
    gap: 8,
    alignItems: "flex-end",
  },
  input: {
    flex: 1,
    background: "rgba(255,255,255,0.04)",
    border: "1px solid rgba(255,255,255,0.08)",
    borderRadius: 8,
    padding: "10px 14px",
    color: "#cdd6f4",
    fontSize: 13,
    fontFamily: "inherit",
    resize: "none" as const,
    outline: "none",
    minHeight: 40,
    maxHeight: 120,
  },
  sendBtn: {
    background: "rgba(137,180,250,0.15)",
    color: "#89b4fa",
    border: "none",
    borderRadius: 8,
    padding: "10px 18px",
    fontSize: 12,
    fontWeight: 600,
    cursor: "pointer",
    flexShrink: 0,
  },
};
