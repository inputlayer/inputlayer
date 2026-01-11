"use client"

import { Clock, Download, Rows3 } from "lucide-react"
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card"
import { Button } from "@/components/ui/button"
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from "@/components/ui/table"
import { Badge } from "@/components/ui/badge"

interface QueryResultsProps {
  data: (string | number | boolean)[][]
  columns: string[]
  executionTime: number
}

export function QueryResults({ data, columns, executionTime }: QueryResultsProps) {
  const handleExport = () => {
    const csv = [columns.join(","), ...data.map((row) => row.join(","))].join("\n")

    const blob = new Blob([csv], { type: "text/csv" })
    const url = URL.createObjectURL(blob)
    const a = document.createElement("a")
    a.href = url
    a.download = "query-results.csv"
    a.click()
    URL.revokeObjectURL(url)
  }

  return (
    <Card>
      <CardHeader className="flex flex-row items-center justify-between space-y-0 pb-3">
        <div>
          <CardTitle className="text-base">Results</CardTitle>
          <CardDescription className="mt-1 flex items-center gap-3">
            <span className="flex items-center gap-1">
              <Rows3 className="h-3 w-3" />
              {data.length} rows
            </span>
            <span className="flex items-center gap-1">
              <Clock className="h-3 w-3" />
              {executionTime}ms
            </span>
          </CardDescription>
        </div>
        <Button variant="outline" size="sm" onClick={handleExport}>
          <Download className="mr-2 h-3 w-3" />
          Export CSV
        </Button>
      </CardHeader>
      <CardContent>
        <div className="rounded-md border overflow-hidden">
          <div className="max-h-[400px] overflow-auto">
            <Table>
              <TableHeader className="sticky top-0 bg-muted">
                <TableRow>
                  <TableHead className="w-12 text-center">#</TableHead>
                  {columns.map((col) => (
                    <TableHead key={col} className="font-mono text-xs">
                      {col}
                    </TableHead>
                  ))}
                </TableRow>
              </TableHeader>
              <TableBody>
                {data.map((row, rowIndex) => (
                  <TableRow key={rowIndex}>
                    <TableCell className="text-center text-muted-foreground text-xs">{rowIndex + 1}</TableCell>
                    {row.map((cell, cellIndex) => (
                      <TableCell key={cellIndex} className="font-mono text-sm">
                        {typeof cell === "boolean" ? (
                          <Badge variant={cell ? "default" : "secondary"}>{cell.toString()}</Badge>
                        ) : (
                          String(cell)
                        )}
                      </TableCell>
                    ))}
                  </TableRow>
                ))}
              </TableBody>
            </Table>
          </div>
        </div>
      </CardContent>
    </Card>
  )
}
