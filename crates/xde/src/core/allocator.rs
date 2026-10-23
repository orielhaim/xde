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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AssignmentCapacity {
    /// Discovery needs one admission slot before a connection is ready.
    Control,
    Transfer {
        serial: u16,
        multiplexed: u16,
        streams: u16,
    },
}
impl AssignmentCapacity {
    pub fn slots(self, connections: u16) -> u16 {
        match self {
            Self::Control => u16::from(connections > 0),
            Self::Transfer {
                serial,
                multiplexed,
                streams,
            } => {
                let multiplexed = multiplexed.min(connections);
                (u32::from(multiplexed) * u32::from(streams)
                    + u32::from(serial.min(connections - multiplexed)))
                .min(u32::from(u16::MAX)) as u16
            }
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub struct Demand {
    pub job: JobId,
    pub order: u64,
    pub origin: OriginId,
    pub connections: u16,
    pub assignments: u16,
    pub capacity: AssignmentCapacity,
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
    if let [d] = eligible.as_slice() {
        let connections = d
            .connections
            .min(limits.max_physical_connections)
            .min(limits.max_connections_per_origin);
        let assignments = if connections > 0 {
            u32::from(d.assignments.min(d.capacity.slots(connections)))
                .min(limits.max_active_assignments) as u16
        } else {
            0
        };
        return vec![(
            d.job,
            ResourceGrant {
                connections,
                assignments,
            },
        )];
    }
    let mut grants = vec![ResourceGrant::default(); eligible.len()];
    let mut origin_ids = slotmap::SecondaryMap::<OriginId, usize>::new();
    let mut origin_used = Vec::new();
    let origins: Vec<_> = eligible
        .iter()
        .map(|d| {
            *origin_ids.entry(d.origin).unwrap().or_insert_with(|| {
                origin_used.push(0u32);
                origin_used.len() - 1
            })
        })
        .collect();
    for connection in [true, false] {
        let budget = if connection {
            u32::from(limits.max_physical_connections)
        } else {
            limits.max_active_assignments
        };
        let want = |i: usize| {
            if connection {
                eligible[i].connections
            } else if grants[i].connections > 0 {
                eligible[i]
                    .assignments
                    .min(eligible[i].capacity.slots(grants[i].connections))
            } else {
                0
            }
        };
        let mut queue = std::collections::BinaryHeap::from(
            eligible
                .iter()
                .enumerate()
                .filter(|(i, _)| want(*i) > 0)
                .map(|(i, d)| Candidate::new(i, 0, d, connection, limits.scheduling))
                .collect::<Vec<_>>(),
        );
        let mut allocated = 0;
        while allocated < budget {
            let Some(candidate) = queue.pop() else { break };
            let i = candidate.index;
            if connection && origin_used[origins[i]] >= u32::from(limits.max_connections_per_origin)
            {
                continue;
            }
            let used = if connection {
                origin_used[origins[i]] += 1;
                grants[i].connections += 1;
                grants[i].connections
            } else {
                grants[i].assignments += 1;
                grants[i].assignments
            };
            allocated += 1;
            let target = if connection {
                eligible[i].connections
            } else {
                eligible[i]
                    .assignments
                    .min(eligible[i].capacity.slots(grants[i].connections))
            };
            if used < target {
                queue.push(Candidate::new(
                    i,
                    used,
                    eligible[i],
                    connection,
                    limits.scheduling,
                ));
            }
        }
    }
    eligible
        .into_iter()
        .zip(grants)
        .map(|(d, g)| (d.job, g))
        .collect()
}

#[derive(Debug)]
struct Candidate {
    index: usize,
    used: u16,
    score: f64,
    service: f64,
    order: u64,
}
impl Candidate {
    fn new(
        index: usize,
        used: u16,
        d: &Demand,
        connection: bool,
        policy: SchedulingPolicy,
    ) -> Self {
        let elasticity = if matches!(policy, SchedulingPolicy::FairShare) {
            if connection {
                d.productivity.connections
            } else {
                d.productivity.assignments
            }
        } else {
            0.0
        };
        Self {
            index,
            used,
            score: f64::from(used) / d.weight / (1.0 + elasticity),
            service: d.service,
            order: d.order,
        }
    }
}
impl Ord for Candidate {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        (other.used != 0)
            .cmp(&(self.used != 0))
            .then_with(|| other.score.total_cmp(&self.score))
            .then_with(|| other.service.total_cmp(&self.service))
            .then(other.order.cmp(&self.order))
            .then(other.index.cmp(&self.index))
    }
}
impl PartialOrd for Candidate {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}
impl PartialEq for Candidate {
    fn eq(&self, other: &Self) -> bool {
        self.cmp(other).is_eq()
    }
}
impl Eq for Candidate {}

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
                capacity: AssignmentCapacity::Transfer {
                    serial: 0,
                    multiplexed: 8,
                    streams: 8,
                },
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

    #[rstest::rstest]
    #[case(4, 7)]
    #[case(2, 5)]
    #[case(1, 4)]
    fn mixed_capacity_is_limited_by_granted_physical_connections(
        #[case] physical: u16,
        #[case] slots: u16,
    ) {
        let mut demand = jobs(1, true);
        demand[0].connections = 4;
        demand[0].assignments = 16;
        demand[0].capacity = AssignmentCapacity::Transfer {
            serial: 3,
            multiplexed: 1,
            streams: 4,
        };
        let mut cap = limits(physical);
        cap.max_active_assignments = 32;
        assert_eq!(
            allocate(&demand, cap)[0].1,
            ResourceGrant {
                connections: physical,
                assignments: slots
            }
        );
        demand[0].capacity = AssignmentCapacity::Transfer {
            serial: 4,
            multiplexed: 0,
            streams: 4,
        };
        assert_eq!(allocate(&demand, cap)[0].1.assignments, physical);
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
