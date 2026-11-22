"""通过 HTTP 端点验证整组派发/中断/幂等重发/续跑全链路。"""
import json
import urllib.parse
import urllib.request

BASE = "http://127.0.0.1:8011/api"


def call(method: str, path: str, payload: dict | None = None) -> dict:
    data = json.dumps(payload).encode() if payload is not None else None
    req = urllib.request.Request(
        BASE + path, data=data, method=method,
        headers={"Content-Type": "application/json"},
    )
    with urllib.request.urlopen(req) as resp:
        return json.loads(resp.read())


def main() -> None:
    gaps = [
        {"分区": "h0", "起始位点": "binlog.002001:100", "截止位点": "binlog.002001:500"},
        {"分区": "h1", "起始位点": "binlog.002001:500", "截止位点": "binlog.002001:900"},
    ]
    first = call("POST", "/checkpoint/1/backfill-batches", {"gaps": gaps})
    batch = first["批次号"]
    h0, h1 = first["details"]
    print("1) 派发:", first["message"])

    # h0 认领并完成
    print("2) h0 认领:", call("POST", f"/backfill_partition/{h0['id']}/claim", {"worker_id": "w-a"})["message"])
    print("   h0 完成回写:", call("POST", f"/backfill_partition/{h0['id']}/progress", {
        "committed_position": "binlog.002001:500",
        "finished_key_range": "id 1000~1860", "finished": True})["message"])

    # h1 认领、写一半、断连中断
    print("3) h1 认领:", call("POST", f"/backfill_partition/{h1['id']}/claim", {"worker_id": "w-b"})["message"])
    call("POST", f"/backfill_partition/{h1['id']}/progress", {
        "committed_position": "binlog.002001:720",
        "finished_key_range": "id 1861~2240"})
    interrupted = call("POST", f"/backfill_partition/{h1['id']}/interrupt", {"reason": "源端连接丢失"})
    print("   h1 中断:", interrupted["message"], "| 状态:", interrupted["entry"]["status"])

    # 重发整批：幂等
    resent = call("POST", "/checkpoint/1/backfill-batches", {"gaps": gaps})
    print("4) 重发整批 idem =", resent["idem"], "|", resent["message"])
    assert resent["idem"] is True and len(resent["details"]) == 2
    h1_again = next(d for d in resent["details"] if d["分区"] == "h1")
    assert h1_again["已提交位点"] == "binlog.002001:720", "重发不得回退已提交位点"

    # 续跑查询
    resume = call("GET", f"/checkpoint/backfill-batches/{batch}")
    print("5) 续跑:", resume["message"])
    assert [d["分区"] for d in resume["claimable"]] == ["h1"]
    assert [d["分区"] for d in resume["finished"]] == ["h0"]

    # 新 worker 认领中断分区，旧 worker 被拒
    claim_c = call("POST", f"/backfill_partition/{h1['id']}/claim", {"worker_id": "w-c"})
    print("6) 新 worker 认领 h1:", claim_c["message"], "| 状态:", claim_c["entry"]["status"])
    claim_d = call("POST", f"/backfill_partition/{h1['id']}/claim", {"worker_id": "w-d"})
    print("   他人再认领被拒 ok =", claim_d["ok"], "|", claim_d["message"])
    assert claim_d["ok"] is False

    # 续跑完成
    done = call("POST", f"/backfill_partition/{h1['id']}/progress", {
        "committed_position": "binlog.002001:900",
        "finished_key_range": "id 1861~2705", "finished": True})
    print("7) h1 续跑完成:", done["entry"]["status"], done["entry"]["已提交位点"])

    # 检查点凭据核对
    cp = call("GET", "/checkpoint/1")
    cred = cp["分区位点凭据"]
    print("8) 检查点分区凭据:", json.dumps(cred, ensure_ascii=False))
    assert cred["h0"]["已提交位点"] == "binlog.002001:500"
    assert cred["h1"]["已提交位点"] == "binlog.002001:900"

    # 明细列表（通用资源接口可筛选）
    query = urllib.parse.urlencode({"批次号": batch})
    listing = call("GET", f"/backfill_partition?{query}")
    print("9) 批次明细数:", listing["total"], "状态:", [i["status"] for i in listing["items"]])

    # 非法参数：空位点由服务层拒绝；缺字段由 Pydantic 拒绝
    bad = call("POST", "/checkpoint/1/backfill-batches",
               {"gaps": [{"分区": "x", "起始位点": "", "截止位点": "z"}]})
    print("10) 空位点服务层拒绝 ok =", bad["ok"], "|", bad["message"])
    assert bad["ok"] is False
    try:
        call("POST", "/checkpoint/1/backfill-batches", {"gaps": [{"分区": "x"}]})
        raise AssertionError("缺字段应当被拒绝")
    except urllib.error.HTTPError as exc:
        print("    缺字段被 Pydantic 拒绝:", exc.code)

    print("\nHTTP 全链路断言全部通过 ✅")


if __name__ == "__main__":
    main()
