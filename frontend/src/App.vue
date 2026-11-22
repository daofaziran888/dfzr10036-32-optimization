<template>
  <div class="app-shell">
    <aside class="app-side">
      <h1 class="app-title">流变工坊 · 实时CDC与流式数据同步平台</h1>
      <nav class="nav-list">
        <RouterLink v-for="item in navItems" :key="item.path" :to="item.path" class="nav-item">
          {{ item.label }}
        </RouterLink>
      </nav>
    </aside>
    <main class="app-main">
      <header class="app-head">
        <span class="head-desc">面向数据库CDC事件捕获、变更日志解析、流式数据转换、维表关联补全、水位线管理、乱序容忍、结果落湖归档与端到端延监控的实时CDC与流式同步平台。</span>
        <span class="head-user">当前值班：数据工程组</span>
      </header>
      <RouterView />
    </main>
  </div>
</template>

<script setup lang="ts">
const navItems = [{ label: "运营概览", path: "/" }, { label: "映射交接", path: "/handoff-workbench" }, { label: "CDC数据源", path: "/cdc_source" }, { label: "日志解析", path: "/binlog_parse" }, { label: "事件转换", path: "/event_trans" }, { label: "维表关联", path: "/dim_join" }, { label: "水位管理", path: "/watermark_mgr" }, { label: "乱序处理", path: "/out_of_order" }, { label: "结果落盘", path: "/sink_writer" }, { label: "延迟监控", path: "/lag_monitor" }, { label: "DDL处理", path: "/ddl_handle" }, { label: "断点恢复", path: "/checkpoint" }, { label: "Schema演进", path: "/schema_evo" }, { label: "流去重", path: "/dedup_stream" }, { label: "湖仓提交", path: "/iceberg_commit" }, { label: "小文件合并", path: "/compaction" }, { label: "快照过期", path: "/expire_snap" }, { label: "流聚合", path: "/metric_aggr" }, { label: "增量回填", path: "/backfill_run" }, { label: "告警规则", path: "/alert_rule" }, { label: "数据比对", path: "/data_diff" }, { label: "编排调度", path: "/task_schedule" }, { label: "流审计", path: "/stream_audit" }, { label: "健康报告", path: "/stream_health" }]
</script>
