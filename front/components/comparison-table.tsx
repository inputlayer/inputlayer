import { CheckCircle, XCircle, Minus, RotateCcw } from "lucide-react"

type CellValue = "native" | "plugin" | "partial" | "manual" | "recompute" | "n/a" | "none"

const cellConfig: Record<CellValue, { icon: React.ReactNode; label?: string }> = {
  native: { icon: <CheckCircle className="h-4 w-4 text-emerald-500" /> },
  plugin: { icon: <CheckCircle className="h-4 w-4 text-yellow-500" />, label: "plugin" },
  partial: { icon: <Minus className="h-4 w-4 text-yellow-500" />, label: "partial" },
  manual: { icon: <Minus className="h-4 w-4 text-yellow-500" />, label: "manual" },
  recompute: { icon: <RotateCcw className="h-4 w-4 text-yellow-500" />, label: "recompute" },
  "n/a": { icon: <Minus className="h-4 w-4 text-muted-foreground/40" />, label: "n/a" },
  none: { icon: <XCircle className="h-4 w-4 text-muted-foreground/40" /> },
}

function isCellValue(value: string): value is CellValue {
  return value in cellConfig
}

function ComparisonCell({ value }: { value: string }) {
  if (!isCellValue(value)) return <span>{value}</span>
  const config = cellConfig[value]
  return (
    <span className="inline-flex flex-col items-center gap-0.5">
      {config.icon}
      {config.label && <span className="text-[10px] text-muted-foreground">{config.label}</span>}
    </span>
  )
}

interface ComparisonRow {
  capability: string
  // A CellValue renders as an icon; any other string renders as text.
  values: Record<string, CellValue | string>
}

interface ComparisonTableProps {
  columns: string[]
  highlightColumn?: string
  rows: ComparisonRow[]
  rowHeader?: string
  align?: "center" | "left"
}

export function ComparisonTable({ columns, highlightColumn, rows, rowHeader = "Capability", align = "center" }: ComparisonTableProps) {
  const alignClass = align === "left" ? "text-left" : "text-center"
  return (
    <div className="overflow-x-auto">
      <table className="w-full border-collapse text-sm">
        <thead>
          <tr className="border-b border-border">
            <th className="text-left py-3 px-4 font-semibold">{rowHeader}</th>
            {columns.map((col) => (
              <th
                key={col}
                className={`${alignClass} py-3 px-4 font-semibold ${
                  col === highlightColumn ? "text-primary" : "text-muted-foreground"
                }`}
              >
                {col}
              </th>
            ))}
          </tr>
        </thead>
        <tbody>
          {rows.map((row) => (
            <tr key={row.capability} className="border-b border-border/50">
              <td className={`py-3 px-4 ${align === "left" ? "align-top font-medium" : ""}`}>{row.capability}</td>
              {columns.map((col) => (
                <td
                  key={col}
                  className={`py-3 px-4 ${alignClass} ${
                    align === "left"
                      ? col === highlightColumn
                        ? "align-top font-medium"
                        : "align-top text-muted-foreground"
                      : ""
                  }`}
                >
                  {align === "left" ? (
                    <ComparisonCell value={row.values[col] || "none"} />
                  ) : (
                    <span className="inline-flex justify-center w-full">
                      <ComparisonCell value={row.values[col] || "none"} />
                    </span>
                  )}
                </td>
              ))}
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  )
}
