import{a as x,e as _,t as f,b as S,d,f as g,v as h,j as l,h as j,l as m,m as v,w as $,n as E,x as q}from"../chunks/scheduler.BpSb_Omi.js";import{S as w,i as y}from"../chunks/index.hIzn_tSQ.js";import{s as C}from"../chunks/entry.DfqrYTBG.js";const H=()=>{const s=C;return{page:{subscribe:s.page.subscribe},navigating:{subscribe:s.navigating.subscribe},updated:s.updated}},P={subscribe(s){return H().page.subscribe(s)}};function k(s){var b;let t,r=s[0].status+"",o,n,i,c=((b=s[0].error)==null?void 0:b.message)+"",u;return{c(){t=_("h1"),o=f(r),n=S(),i=_("p"),u=f(c)},l(e){t=d(e,"H1",{});var a=g(t);o=h(a,r),a.forEach(l),n=j(e),i=d(e,"P",{});var p=g(i);u=h(p,c),p.forEach(l)},m(e,a){m(e,t,a),v(t,o),m(e,n,a),m(e,i,a),v(i,u)},p(e,[a]){var p;a&1&&r!==(r=e[0].status+"")&&$(o,r),a&1&&c!==(c=((p=e[0].error)==null?void 0:p.message)+"")&&$(u,c)},i:E,o:E,d(e){e&&(l(t),l(n),l(i))}}}function z(s,t,r){let o;return q(s,P,n=>r(0,o=n)),[o]}let F=class extends w{constructor(t){super(),y(this,t,z,k,x,{})}};export{F as component};