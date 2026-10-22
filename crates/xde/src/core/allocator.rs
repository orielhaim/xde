//! Coarse, engine-global resource grants. Actual objects remain in WorldModel.
use super::{
    ids::{JobId, OriginId},
    policy::{EngineLimits, SchedulingPolicy},
};

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct ResourceGrant {
    pub connections: u16,
    pub assignments: u16,
}

#[derive(Debug, Clone, Copy)]
pub struct Demand {
    pub job: JobId,
    pub order: u64,
    pub origin: OriginId,
    pub connections: u16,
    pub assignments: u16,
    pub weight: f64,
    pub service: f64,
    pub active: bool,
    pub productivity: super::planner::Productivity,
}

/// Weighted max-min admission for divisible slots; byte service breaks indivisible ties.
/// No grants are fabricated for idle demand. Pending work is included in service.
pub fn allocate(demand: &[Demand], limits: EngineLimits) -> Vec<(JobId, ResourceGrant)> {
    let mut eligible: Vec<_> = demand.iter().collect();
    if let SchedulingPolicy::Queue { max_active_jobs } = limits.scheduling {
        eligible.sort_by(|a, b| {
            b.active
                .cmp(&a.active)
                .then_with(|| b.weight.total_cmp(&a.weight))
                .then(a.order.cmp(&b.order))
        });
        eligible.truncate(max_active_jobs.get());
    }
    let mut grants = vec![ResourceGrant::default(); eligible.len()];
    for connection in [true, false] {
        let budget = if connection {
            u32::from(limits.max_physical_connections)
        } else {
            limits.max_active_assignments
        };
        for _ in 0..budget {
            let selected = eligible
                .iter()
                .enumerate()
                .filter(|(i, d)| {
                    let g = grants[*i];
                    let used = if connection {
                        g.connections
                    } else {
                        g.assignments
                    };
                    let want = if connection {
                        d.connections
                    } else {
                        d.assignments
                    };
                    if used >= want || (!connection && g.connections == 0) {
                        return false;
                    }
                    !connection
                        || eligible
                            .iter()
                            .enumerate()
                            .filter(|(_, peer)| peer.origin == d.origin)
                            .map(|(n, _)| u32::from(grants[n].connections))
                            .sum::<u32>()
                            < u32::from(limits.max_connections_per_origin)
                })
                .min_by(|(i, a), (j, b)| {
                    let used = |n: usize| {
                        if connection {
                            grants[n].connections
                        } else {
                            grants[n].assignments
                        }
                    };
                    let x = used(*i);
                    let y = used(*j);
                    // Give every runnable job an opportunity before increasing parallelism.
                    (x != 0)
                        .cmp(&(y != 0))
                        .then_with(|| {
                            if x == 0 && y == 0 {
                                a.service.total_cmp(&b.service)
                            } else {
                                let score = |used: u16, d: &Demand| {
                                    let elasticity =
                                        if matches!(limits.scheduling, SchedulingPolicy::FairShare)
                                        {
                                            if connection {
                                                d.productivity.connections
                                            } else {
                                                d.productivity.assignments
                                            }
                                        } else {
                                            0.0
                                        };
                                    // d log(n * goodput(n))/dn = (1 + elasticity)/n.
                                    f64::from(used) / d.weight / (1.0 + elasticity)
                                };
                                score(x, a).total_cmp(&score(y, b))
                            }
                        })
                        .then_with(|| a.service.total_cmp(&b.service))
                        .then(a.order.cmp(&b.order))
                })
                .map(|(i, _)| i);
            let Some(i) = selected else {
                break;
            };
            if connection {
                grants[i].connections += 1;
            } else {
                grants[i].assignments += 1;
            }
        }
    }
    eligible
        .into_iter()
        .zip(grants)
        .map(|(d, g)| (d.job, g))
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;
    use slotmap::SlotMap;

    fn jobs(n: usize, shared: bool) -> Vec<Demand> {
        let mut ids = SlotMap::<JobId, ()>::with_key();
        let mut origins = SlotMap::<OriginId, ()>::with_key();
        let origin = origins.insert(());
        (0..n)
            .map(|order| Demand {
                order: order as u64,
                job: ids.insert(()),
                origin: if shared { origin } else { origins.insert(()) },
                connections: 8,
                assignments: 8,
                weight: 1.0,
                service: 0.0,
                active: false,
                productivity: Default::default(),
            })
            .collect()
    }
    fn limits(cap: u16) -> EngineLimits {
        EngineLimits {
            max_physical_connections: cap,
            max_connections_per_origin: cap,
            max_active_assignments: u32::from(cap),
            ..Default::default()
        }
    }
    #[test]
    fn work_conserving_grants_arrivals_and_idle_demand() {
        for shared in [false, true] {
            let mut demand = jobs(3, shared);
            assert_eq!(
                allocate(&demand[..1], limits(8))[0].1,
                ResourceGrant {
                    connections: 8,
                    assignments: 8
                }
            );
            let two = allocate(&demand[..2], limits(8));
            assert_eq!(
                two.iter().map(|(_, g)| g.assignments).collect::<Vec<_>>(),
                [4, 4]
            );
            let three = allocate(&demand, limits(8));
            assert!(three.iter().all(|(_, g)| g.assignments >= 2));
            demand[0].connections = 1;
            demand[0].assignments = 1;
            let borrowed = allocate(&demand[..2], limits(8));
            assert_eq!(
                borrowed
                    .iter()
                    .map(|(_, g)| g.assignments)
                    .collect::<Vec<_>>(),
                [1, 7]
            );
            demand.remove(0);
            assert_eq!(allocate(&demand[..1], limits(8))[0].1.assignments, 8);
        }
    }
    #[test]
    fn productive_surplus_is_bounded_and_does_not_replace_priority() {
        let mut demand = jobs(2, false);
        demand[0].productivity = super::super::planner::Productivity {
            connections: 0.0,
            assignments: 0.0,
        };
        let grants = allocate(&demand, limits(8));
        assert_eq!(
            grants
                .iter()
                .map(|(_, g)| g.assignments)
                .collect::<Vec<_>>(),
            [3, 5]
        );
        demand[0].weight = 4.0;
        let grants = allocate(&demand, limits(8));
        assert!(grants[0].1.assignments > grants[1].1.assignments);
        assert!(grants.iter().all(|(_, g)| g.assignments > 0));
        let utility = |n: f64| n.ln() + 2.0 * (8.0 - n).ln();
        let oracle = (1..8)
            .map(|n| utility(f64::from(n)))
            .fold(f64::NEG_INFINITY, f64::max);
        assert!((utility(3.0) - oracle).abs() < 1e-12);
    }

    #[test]
    fn byte_service_simulator_has_weighted_turns_without_starvation() {
        let mut demand = jobs(3, false);
        demand[0].weight = 4.0;
        let mut counts = [0usize; 3];
        for _ in 0..600 {
            for (job, grant) in allocate(&demand, limits(1)) {
                if grant.assignments == 0 {
                    continue;
                }
                let i = demand.iter().position(|d| d.job == job).unwrap();
                counts[i] += 1;
                demand[i].service += 1.0 / demand[i].weight;
            }
        }
        assert_eq!(counts, [400, 100, 100]);
        // New job starts at the active virtual-service floor, as admission does.
        let mut newcomer = jobs(4, false).pop().unwrap();
        newcomer.service = demand
            .iter()
            .map(|d| d.service)
            .fold(f64::INFINITY, f64::min);
        let id = newcomer.job;
        demand.push(newcomer);
        let mut served = false;
        for _ in 0..8 {
            for (job, grant) in allocate(&demand, limits(1)) {
                if grant.assignments == 0 {
                    continue;
                }
                served |= job == id;
                let d = demand.iter_mut().find(|d| d.job == job).unwrap();
                d.service += 1.0 / d.weight;
            }
        }
        assert!(served);
    }

    #[test]
    fn indivisible_surplus_rotates_with_byte_service() {
        let mut demand = jobs(2, false);
        let mut service = [0u64; 2];
        for _ in 0..100 {
            for (job, grant) in allocate(&demand, limits(3)) {
                let i = demand.iter().position(|d| d.job == job).unwrap();
                service[i] += u64::from(grant.assignments);
                demand[i].service += f64::from(grant.assignments);
            }
        }
        assert_eq!(service, [150, 150]);
    }
    #[test]
    fn queue_is_nonpreemptive_priority_ordered_and_releases_on_removal() {
        let mut demand = jobs(3, true);
        demand[1].weight = 4.0;
        demand[2].weight = 8.0;
        demand[0].active = true;
        let mut cap = limits(8);
        cap.scheduling = SchedulingPolicy::Queue {
            max_active_jobs: 1.try_into().unwrap(),
        };
        assert_eq!(allocate(&demand, cap)[0].0, demand[0].job);
        demand.remove(0);
        assert_eq!(allocate(&demand, cap)[0].0, demand[1].job);
        cap.scheduling = SchedulingPolicy::Queue {
            max_active_jobs: 2.try_into().unwrap(),
        };
        assert_eq!(allocate(&demand, cap).len(), 2);
    }
    proptest! {
        #[test]
        fn grants_conserve_resources_and_origin_caps(n in 1usize..12, cap in 1u16..32, origin_cap in 1u16..16, want in 0u16..10) {
            let mut demand = jobs(n, true);
            for d in &mut demand { d.connections = want; d.assignments = want; }
            let mut budget = limits(cap);
            budget.max_connections_per_origin = origin_cap;
            let grants = allocate(&demand, budget);
            let conns: u16 = grants.iter().map(|(_, g)| g.connections).sum();
            let slots: u16 = grants.iter().map(|(_, g)| g.assignments).sum();
            prop_assert!(conns <= cap.min(origin_cap));
            prop_assert!(u32::from(slots) <= budget.max_active_assignments);
            prop_assert!(grants.iter().all(|(_, g)| g.connections <= want && g.assignments <= want && (g.connections != 0 || g.assignments == 0)));
        }
    }
}
