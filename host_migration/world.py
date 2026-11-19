"""Deterministic tick-based world: players plus mechanisms (platform, door, gear).

Everything that must survive host migration lives in the snapshot: object
identities, in-progress multi-tick actions, mechanism state and the sim clock.
"""

from __future__ import annotations

import copy

TICK_DT = 0.05  # seconds per simulation tick

ARENA_W = 480
ARENA_H = 320


def _player(pid: str, idx: int) -> dict:
    return {
        "id": pid,
        "x": 40.0 + idx * 36.0,
        "y": 280.0,
        "vx": 0.0,
        # in-progress multi-tick action, e.g. {"kind": "press", "remaining": 5}
        "action": None,
    }


class World:
    def __init__(self, session_id: str):
        self.session = session_id
        self.tick = 0
        self.players: dict[str, dict] = {}
        # mechanisms / moving parts
        self.platform = {"id": "plat1", "x": 60.0, "y": 240.0,
                         "x0": 60.0, "x1": 300.0, "speed": 2.0, "dir": 1}
        self.door = {"id": "door1", "open": 0, "target": 0, "speed": 2}
        self.button = {"id": "btn1", "x": 120.0, "y": 280.0,
                       "door_id": "door1", "pressed_until": -1}
        self.gear = {"id": "gear1", "x": 420.0, "y": 60.0,
                     "angle": 0.0, "speed": 3.0}
        self.press_count = 0  # makes double-execution of inputs visible

    @classmethod
    def genesis(cls, session_id: str) -> "World":
        return cls(session_id)

    def add_player(self, pid: str) -> None:
        if pid not in self.players:
            self.players[pid] = _player(pid, len(self.players))

    def remove_player(self, pid: str) -> None:
        self.players.pop(pid, None)

    # ---- simulation -----------------------------------------------------

    def apply_input(self, pid: str, action: str) -> None:
        p = self.players.get(pid)
        if p is None:
            return
        if p["action"] is not None:
            return  # busy with a multi-tick action
        if action == "move_left":
            p["vx"] = -2.0
        elif action == "move_right":
            p["vx"] = 2.0
        elif action == "stop":
            p["vx"] = 0.0
        elif action == "press":
            p["action"] = {"kind": "press", "remaining": 5}
            p["vx"] = 0.0

    def step(self) -> None:
        self.tick += 1
        # players: movement + in-progress actions
        for p in self.players.values():
            act = p["action"]
            if act is not None:
                act["remaining"] -= 1
                if act["remaining"] <= 0:
                    if act["kind"] == "press":
                        self.button["pressed_until"] = self.tick + 120
                        self.press_count += 1
                    p["action"] = None
            else:
                p["x"] = min(max(p["x"] + p["vx"], 10.0), ARENA_W - 10.0)
        # platform: ping-pong between x0 and x1
        plat = self.platform
        plat["x"] += plat["speed"] * plat["dir"]
        if plat["x"] >= plat["x1"]:
            plat["x"] = plat["x1"]
            plat["dir"] = -1
        elif plat["x"] <= plat["x0"]:
            plat["x"] = plat["x0"]
            plat["dir"] = 1
        # door: target follows button window
        self.door["target"] = 100 if self.tick < self.button["pressed_until"] else 0
        door = self.door
        if door["open"] < door["target"]:
            door["open"] = min(door["open"] + door["speed"], door["target"])
        elif door["open"] > door["target"]:
            door["open"] = max(door["open"] - door["speed"], door["target"])
        # gear: free-running moving part
        self.gear["angle"] = (self.gear["angle"] + self.gear["speed"]) % 360.0

    # ---- snapshot / restore --------------------------------------------

    def snapshot(self) -> dict:
        return {
            "session": self.session,
            "tick": self.tick,
            "players": copy.deepcopy(self.players),
            "platform": copy.deepcopy(self.platform),
            "door": copy.deepcopy(self.door),
            "button": copy.deepcopy(self.button),
            "gear": copy.deepcopy(self.gear),
            "press_count": self.press_count,
        }

    @classmethod
    def from_snapshot(cls, snap: dict) -> "World":
        w = cls.__new__(cls)
        w.session = snap["session"]
        w.tick = snap["tick"]
        w.players = copy.deepcopy(snap["players"])
        w.platform = copy.deepcopy(snap["platform"])
        w.door = copy.deepcopy(snap["door"])
        w.button = copy.deepcopy(snap["button"])
        w.gear = copy.deepcopy(snap["gear"])
        w.press_count = snap["press_count"]
        return w

    def entity_ids(self) -> list[str]:
        return (list(self.players)
                + [self.platform["id"], self.door["id"],
                   self.button["id"], self.gear["id"]])
