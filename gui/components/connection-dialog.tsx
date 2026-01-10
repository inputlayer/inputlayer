"use client"

import { useState } from "react"
import { Server, Loader2 } from "lucide-react"
import { Button } from "@/components/ui/button"
import {
  Dialog,
  DialogContent,
  DialogDescription,
  DialogFooter,
  DialogHeader,
  DialogTitle,
  DialogTrigger,
} from "@/components/ui/dialog"
import { Input } from "@/components/ui/input"
import { Label } from "@/components/ui/label"
import { useDatalogStore } from "@/lib/datalog-store"

export function ConnectionDialog() {
  const [open, setOpen] = useState(false)
  const [host, setHost] = useState("localhost")
  const [port, setPort] = useState("8080")
  const [name, setName] = useState("My Datalog Server")
  const [isConnecting, setIsConnecting] = useState(false)

  const { connect, connection } = useDatalogStore()

  const handleConnect = async () => {
    setIsConnecting(true)
    try {
      await connect(host, Number.parseInt(port), name)
      setOpen(false)
    } finally {
      setIsConnecting(false)
    }
  }

  return (
    <Dialog open={open} onOpenChange={setOpen}>
      <DialogTrigger asChild>
        <Button>
          <Server className="mr-2 h-4 w-4" />
          {connection ? "Change Connection" : "Connect to Server"}
        </Button>
      </DialogTrigger>
      <DialogContent className="sm:max-w-md">
        <DialogHeader>
          <DialogTitle>Connect to Datalog Server</DialogTitle>
          <DialogDescription>Enter the connection details for your Datalog server</DialogDescription>
        </DialogHeader>
        <div className="grid gap-4 py-4">
          <div className="grid gap-2">
            <Label htmlFor="name">Connection Name</Label>
            <Input id="name" value={name} onChange={(e) => setName(e.target.value)} placeholder="My Datalog Server" />
          </div>
          <div className="grid grid-cols-3 gap-4">
            <div className="col-span-2 grid gap-2">
              <Label htmlFor="host">Host</Label>
              <Input id="host" value={host} onChange={(e) => setHost(e.target.value)} placeholder="localhost" />
            </div>
            <div className="grid gap-2">
              <Label htmlFor="port">Port</Label>
              <Input
                id="port"
                type="number"
                value={port}
                onChange={(e) => setPort(e.target.value)}
                placeholder="8080"
              />
            </div>
          </div>
        </div>
        <DialogFooter>
          <Button variant="outline" onClick={() => setOpen(false)}>
            Cancel
          </Button>
          <Button onClick={handleConnect} disabled={isConnecting}>
            {isConnecting && <Loader2 className="mr-2 h-4 w-4 animate-spin" />}
            {isConnecting ? "Connecting..." : "Connect"}
          </Button>
        </DialogFooter>
      </DialogContent>
    </Dialog>
  )
}
