"use client"

import { Network, Rows3, Columns3 } from "lucide-react"
import { Card } from "@/components/ui/card"
import { Badge } from "@/components/ui/badge"
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from "@/components/ui/table"
import type { Relation } from "@/lib/datalog-store"

interface RelationDetailProps {
  relation: Relation
}

export function RelationDetail({ relation }: RelationDetailProps) {
  return (
    <Card>
      <div className="p-6 space-y-6">
        <div className="flex items-center justify-between">
          <div>
            <h2 className="text-xl font-semibold flex items-center gap-2">
              <Network className="h-5 w-5 text-primary" />
              {relation.name}
            </h2>
            <p className="text-sm text-muted-foreground mt-1">Base Relation • Arity {relation.arity}</p>
          </div>
          <div className="flex gap-4 text-sm">
            <div className="flex items-center gap-1.5 text-muted-foreground">
              <Columns3 className="h-4 w-4" />
              {relation.columns.length} columns
            </div>
            <div className="flex items-center gap-1.5 text-muted-foreground">
              <Rows3 className="h-4 w-4" />
              {relation.tupleCount.toLocaleString()} tuples
            </div>
          </div>
        </div>

        <div>
          <h3 className="text-sm font-medium mb-2">Schema</h3>
          <div className="flex flex-wrap gap-2">
            {relation.columns.map((col, index) => (
              <Badge key={col} variant="secondary" className="font-mono">
                <span className="text-muted-foreground mr-1">{index}:</span>
                {col}
              </Badge>
            ))}
          </div>
        </div>

        <div>
          <h3 className="text-sm font-medium mb-2">Sample Data</h3>
          <div className="rounded-md border overflow-hidden">
            <div className="max-h-[300px] overflow-auto">
              <Table>
                <TableHeader className="sticky top-0 bg-muted">
                  <TableRow>
                    {relation.columns.map((col) => (
                      <TableHead key={col} className="font-mono text-xs">
                        {col}
                      </TableHead>
                    ))}
                  </TableRow>
                </TableHeader>
                <TableBody>
                  {relation.data.map((row, rowIndex) => (
                    <TableRow key={rowIndex}>
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
          <p className="text-xs text-muted-foreground mt-2">
            Showing {relation.data.length} of {relation.tupleCount.toLocaleString()} tuples
          </p>
        </div>
      </div>
    </Card>
  )
}
