import {switchover} from '../api/business.js';
import {esc} from '../utils/html.js';
import {table} from '../components/common/table.js';

const NODE_LABEL={primary:'主库 primary',standby:'副库 standby'};
const PHASE_LABEL={idle:'空闲',queued:'已排队',syncing:'副库追平中',draining:'交接停写中',flipping:'翻转写指针'};

export function switchPage(snap){
 if(!snap)return '<div class="empty">正在读取双库状态…</div>';
 const writer=snap.writer_node;
 return `<div class="switch-board">
  <section class="switch-hero">
   <div><span class="eyebrow">DB TOPOLOGY / WRITE-READ ROUTING</span>
   <h2>主副库交接台</h2>
   <p>检验库维护时在此发起切换；副库追平前主库照常接单，交接瞬间短暂停写，查询始终可用。</p></div>
   <div class="writer-badge ${writer}"><small>当前写入位置</small><strong>${esc(NODE_LABEL[writer]||writer)}</strong><em>term ${snap.term} · ${esc(PHASE_LABEL[snap.phase]||snap.phase)}</em></div>
  </section>

  <div class="metrics">
   <div><small>写库节点</small><strong>${esc(NODE_LABEL[writer])}</strong></div>
   <div><small>本次查询读库</small><strong>${esc(NODE_LABEL[snap.reader_node]||snap.reader_node)}</strong></div>
   <div><small>副库复制延迟</small><strong>${snap.replication_lag==null?'—':snap.replication_lag}<em>条日志</em></strong></div>
   <div><small>写库最大 LSN</small><strong>${snap.writer_max_lsn==null?'—':snap.writer_max_lsn}</strong></div>
  </div>

  ${snap.stuck_reason?`<div class="feedback warn"><b>卡住原因：</b>${esc(snap.stuck_reason)}</div>`:''}
  ${snap.phase!=='idle'?`<div class="feedback info">交接阶段 <b>${esc(PHASE_LABEL[snap.phase]||snap.phase)}</b>：${esc(snap.block_reason||'进行中')}</div>`:''}

  <section class="node-grid">
   ${snap.nodes?Object.entries(snap.nodes).map(([name,n])=>`
    <article class="node-card ${n.healthy?'healthy':'down'} ${n.role==='writer'?'is-writer':''}">
     <header><b>${esc(NODE_LABEL[name]||name)}</b><span class="status ${n.healthy?'completed':'failed'}">${n.healthy?'健康':'中断'}</span></header>
     <p>角色：${n.role==='writer'?'当前写库（接写）':'副库（只读副本）'}</p>
     <p>已应用 LSN：${n.applied_lsn==null?'—':n.applied_lsn}</p>
    </article>`).join(''):''}
  </section>

  <section class="switch-actions">
   <h3>发起主副库切换</h3>
   <p>目标：${esc(NODE_LABEL[snap.target_node]||snap.target_node)}。同一切换单重复提交会幂等复用，不会重复换主；中断后自动从断点续跑。</p>
   <button data-switch-initiate ${snap.phase!=='idle'?'disabled':''}>切换到${esc(NODE_LABEL[snap.target_node]||snap.target_node)} →</button>
   ${snap.active_order_id?`<small>活动切换单 #${snap.active_order_id}</small>`:''}
  </section>

  <section><h3>切换记录</h3><div id="switch-orders">读取中…</div></section>

  <details class="fault-panel"><summary>演练用故障注入（仅本地/验收环境）</summary>
   <p>模拟节点网络中断与恢复，验证旧库重新上线后不能接写、切换可中断续跑。</p>
   <button data-fault="primary:true">主库下线</button>
   <button data-fault="primary:false">主库恢复</button>
   <button data-fault="standby:true">副库下线</button>
   <button data-fault="standby:false">副库恢复</button>
  </details>
 </div>`;
}

export async function loadOrders(){
 try{
  const rows=await switchover.orders();
  document.getElementById('switch-orders').innerHTML=table(
   ['切换单 / 时间','方向','状态 / 任期','回执'],
   rows.map(o=>[`<b>#${o.id} ${esc(o.requester)}</b><small>${esc(o.created_at||'')}</small>`,
    `${esc(NODE_LABEL[o.from_node]||o.from_node)} → ${esc(NODE_LABEL[o.target_node]||o.target_node)}`,
    `<span class="status ${o.status==='completed'?'completed':o.status==='failed'?'failed':'running'}">${esc(o.status)}</span><small>term ${o.term}</small>`,
    o.completed_at?esc(o.completed_at):(o.detail&&o.detail.paused_reason?esc(o.detail.paused_reason):'—')]));
 }catch(e){
  document.getElementById('switch-orders').innerHTML=`<div class="empty">${esc(e.message)}</div>`;
 }
}
