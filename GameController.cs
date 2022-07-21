using Game.Common;
using Game.Design;
using Game.Model;
using Google.Authenticator;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Options;
using NETCore.MailKit.Core;
using NETCore.MailKit.Infrastructure.Internal;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using Quartz;
using SqlSugar;
using System;
using System.Collections.Generic;
using System.Configuration;
using System.Data;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Linq.Expressions;
using System.Net;
using System.Net.Mail;
using System.Threading;
using System.Threading.Tasks;
using Telegram.Bot;
using Telegram.Bot.Types;
using Telegram.Bot.Types.Enums;
using Telegram.Bot.Types.ReplyMarkups;

namespace Game.Controllers
{
    [Route("api/[controller]/[action]")]
    [ApiController]
    public class GameController : ControllerBase
    {
        public GameController(ICsRedisCache _redis, ISqlSugarClient _db, IConfiguration _configuration, ISchedulerFactory schedulerFactory, IOptions<EmailOptions> options, IEmailService emailService)
        {
            _schedulerfactory=schedulerFactory;
            redis = _redis;
            db = _db;
            configuration = _configuration;
            this.options = options.Value;
            _EmailService = emailService;
        }
        private readonly ISchedulerFactory _schedulerfactory;
        private  IScheduler _scheduler;
        private ICsRedisCache redis { get; }
        private ISqlSugarClient db { get; }
        private IConfiguration configuration { get; }
        private readonly EmailOptions options;
        private readonly IEmailService _EmailService;

        [HttpPost]
        public async Task StartQuartz()
        {
            _scheduler= await _schedulerfactory.GetScheduler();
            await _scheduler.Start();
            var trigger = TriggerBuilder.Create()
                           .WithSimpleSchedule(x => x.WithIntervalInSeconds(10).RepeatForever())//每10秒执行一次
                           .Build();
            //创建作业实例
            //Jobs即我们需要执行的作业
            var jobDetail = JobBuilder.Create<GameQuartz>()
                            .Build();
            //将触发器和作业任务绑定到调度器中
            await _scheduler.ScheduleJob(jobDetail, trigger);
        }

        
        /// <summary>
        /// 创建实体
        /// </summary>
        [HttpGet]
        public void CreateModel()
        {
            db.DbFirst.IsCreateAttribute().CreateClassFile("c:\\Demo\\game", "Game.Model");
        }

        /// <summary>
        /// 获取验证码
        /// </summary>
        /// <returns></returns>
        [HttpGet]
        public MsgInfo<string> GetCode()
        {
            MsgInfo<string> info = new();
            DateTime t = DateTime.Now;

            string code = YRHelper.GetRandom(4);
            redis.Set(code, code, 60);

            info.code = 200;
            info.msg = "success";
            info.data = code;

            return info;
        }

        /// <summary>
        /// 管理员/会员登录(UserType:0为管理员用户，1为会员用户)
        /// </summary>
        /// <param name="param">{"UserName":"zs","Pwd":"123456","Code":"1234","UserType":"0"}</param>
        /// <returns></returns>
        [HttpPost]
        public MsgInfo<RetuLoginUser> Login(dynamic param)
        {
            MsgInfo<RetuLoginUser> info = new();
            var data = JsonConvert.DeserializeObject<dynamic>(param.ToString());
            DateTime t = DateTime.Now;

            //AdminUser
            string OpenId = CreateOpenId();
            string TJCode = CreateTJCode();

            if (Object.ReferenceEquals(null, data))
            {
                info.code = 400;
                info.msg = "Network Exception";
                return info;
            }

            var ym = Request.Headers["Referer"].ToString().Replace("https://", "").Replace("http://", "").Replace("/", "");
            LogHelper.Debug(ym);
            //var ym = "localhost:44396";

            string UserName = data.UserName.ToString();
            string Pwd = YRHelper.get_md5_32(data.Pwd.ToString());
            string Code = data.Code.ToString();
            string UserType = data.UserType.ToString();

            if (!redis.Exists(Code.ToUpper()))
            {
                info.code = 400;
                info.msg = "Verification code error";
                return info;
            }
            if (redis.Get(Code.ToUpper()).ToUpper().Trim() != Code.ToUpper().Trim())
            {
                info.code = 400;
                info.msg = "Verification code error";
                return info;
            }

            long PassportId = 0;
            long ymPassportId = 1;
            var yms = db.Queryable<OperateSys>().First(d => d.ShopDomain.Contains(ym) || d.AgentDomain.Contains(ym));
            if (yms != null)
            {
                ymPassportId = yms.PassportId;
            }

            if (UserType == "0")
            {
                AdminUser adminUser = db.Queryable<AdminUser>().First(c => c.UserName == UserName && c.Pwd == Pwd && c.IsValid == 1);
                if (adminUser == null)
                {
                    int? current_error_count = adminUser.LoginErrorCount + 1;
                    if (current_error_count >= 5)
                    {
                        info.code = 400;
                        info.msg = $"Account has been disabled";
                        return info;
                    }
                    #region 连续错误5次禁用账户
                    adminUser.UpdateTime = DateTime.Now;
                    adminUser.LoginErrorCount += 1;
                    if (adminUser.LoginErrorCount == 5)
                        adminUser.IsValid = 0;
                    db.Updateable(adminUser).ExecuteCommand();
                    #endregion

                    info.code = 400;
                    info.msg = $"The account will be disabled after 5 consecutive login errors, and there are {current_error_count} remaining";
                    return info;
                }
                PassportId = adminUser.PassportId;
                if (PassportId != 999999999)
                {
                    PassportId = adminUser.DominPassportId;
                    if (ymPassportId != PassportId)
                    {
                        info.code = 400;
                        info.msg = "account or pwd's wrong";
                        return info;
                    }
                }

                RetuLoginUser userInfo = db.Queryable<AdminUser>().Where(c => c.UserName == UserName && c.Pwd == Pwd &&
                c.IsValid == 1).Select(c => new RetuLoginUser()
                {
                    IsValid = c.IsValid,
                    OpenId = c.OpenId,
                    PassportId = c.PassportId,
                    HashAddress = c.HashAddress,
                    RoleId = c.RoleId,
                    UserName = c.UserName,
                    AdminPassportId = c.AdminPassportId
                }).First();

                userInfo.RoleType = db.Queryable<AdminRole>().First(c => c.Id == userInfo.RoleId).RoleType;
                List<RetuAdminPage> allPages = db.Queryable<AdminPage>()
                .LeftJoin<AdminPower>((o, i) => o.Id == i.PageId &&
                (o.ManageUserPassportId == userInfo.PassportId || o.ManageUserPassportId == userInfo.AdminPassportId))
                .Where((o, i) => i.RoleId == userInfo.RoleId)
                .Select((o, i) => new RetuAdminPage
                {
                    Id = o.Id,
                    PageName = o.PageName,
                    PageUrl = o.PageUrl,
                    PageIcon = o.PageIcon,
                    icon = o.PageIcon,
                    ParentId = o.ParentId,
                    RoleId = i.RoleId
                })
                .ToList();
                List<RetuAdminPage> pages = db.Queryable<AdminPage>()
                .LeftJoin<AdminPower>((o, i) => o.Id == i.PageId &&
                (o.ManageUserPassportId == userInfo.PassportId || o.ManageUserPassportId == userInfo.AdminPassportId))
                .Where((o, i) => i.RoleId == userInfo.RoleId && o.ParentId == 0)
                .Select((o, i) => new RetuAdminPage
                {
                    Id = o.Id,
                    PageName = o.PageName,
                    PageUrl = o.PageUrl,
                    PageIcon = o.PageIcon,
                    icon = o.PageIcon,
                    ParentId = o.ParentId,
                    RoleId = i.RoleId
                })
                .ToList();

                pages.ForEach(x => x.Childrens = allPages.Where(c => c.ParentId == x.Id).Select(o => new RetuAdminPage()
                {
                    Id = o.Id,
                    PageName = o.PageName,
                    PageUrl = o.PageUrl,
                    PageIcon = o.PageIcon,
                    icon = o.PageIcon,
                    ParentId = o.ParentId,
                    RoleId = o.RoleId
                }).ToList());
                userInfo.pages = pages;

                int i = db.Updateable<AdminUser>().SetColumns(c => c.UpdateTime == t).
                      //SetColumns(c => c.OpenId == YRHelper.get_md5_32(OpenId)).
                      SetColumns(c => c.IsValid == 1).
                      SetColumns(c => c.CurrentTimeStamp == convert_time_int_to10(t)).
                      SetColumns(c => c.LoginErrorCount == 0).
                      Where(c => c.PassportId == userInfo.PassportId && c.UserName == UserName).
                      ExecuteCommand();
                if (i < 1)
                {
                    info.code = 400;
                    info.msg = "Network Exception";
                    return info;
                }
                //userInfo.OpenId = YRHelper.get_md5_32(OpenId);
                info.code = 200;
                info.msg = "success";
                info.data = userInfo;
            }
            else if (UserType == "1")
            {
                UserInfo userInfo1 = db.Queryable<UserInfo>().First(c => c.UserName == UserName && c.Pwd == Pwd && c.IsValid == 1);
                if (userInfo1 == null)
                {
                    int? current_error_count = userInfo1.LoginErrorCount + 1;
                    if (current_error_count >= 5)
                    {
                        info.code = 400;
                        info.msg = $"Account has been disabled";
                        return info;
                    }
                    #region 连续错误5次禁用账户
                    userInfo1.UpdateTime = DateTime.Now;
                    userInfo1.LoginErrorCount += 1;
                    if (userInfo1.LoginErrorCount == 5)
                        userInfo1.IsValid = 0;
                    db.Updateable(userInfo1).ExecuteCommand();
                    #endregion

                    info.code = 400;
                    info.msg = $"The account will be disabled after 5 consecutive login errors, and there are {current_error_count} remaining";
                    return info;
                }
                var ManageUserPassportId = userInfo1.ManageUserPassportId;
                var DominPassportId = db.Queryable<AdminUser>().First(c => c.PassportId == ManageUserPassportId).DominPassportId;
                if (ymPassportId != DominPassportId)
                {
                    info.code = 400;
                    info.msg = "account or pwd's wrong";
                    return info;
                }

                RetuLoginUser userInfo = db.Queryable<UserInfo>().Where(c => c.HashAddress == UserName && c.Pwd == Pwd &&
                c.IsValid == 1).Select(c => new RetuLoginUser()
                {
                    IsValid = c.IsValid,
                    OpenId = c.OpenId,
                    PassportId = long.Parse(c.ManageUserPassportId.ToString()),
                    HashAddress = c.HashAddress,
                    RoleId = c.RoleId,
                    UserName = c.UserName,
                    AgentTypeID = c.AgentTypeID
                }).First();
                userInfo.RoleType = db.Queryable<AdminRole>().First(c => c.Id == userInfo.RoleId).RoleType;
                List<RetuAdminPage> allPages = db.Queryable<AdminPage>()
                                .LeftJoin<AdminPower>((o, i) => o.Id == i.PageId && o.ManageUserPassportId == userInfo.PassportId)
                                .Where((o, i) => i.RoleId == userInfo.RoleId)
                                .Select((o, i) => new RetuAdminPage
                                {
                                    Id = o.Id,
                                    PageName = o.PageName,
                                    PageUrl = o.PageUrl,
                                    PageIcon = o.PageIcon,
                                    icon = o.PageIcon,
                                    ParentId = o.ParentId,
                                    RoleId = i.RoleId
                                })
                                .ToList();
                List<RetuAdminPage> pages = db.Queryable<AdminPage>()
                .LeftJoin<AdminPower>((o, i) => o.Id == i.PageId && o.ManageUserPassportId == userInfo.PassportId)
                .Where((o, i) => i.RoleId == userInfo.RoleId && o.ParentId == 0)
                .Select((o, i) => new RetuAdminPage
                {
                    Id = o.Id,
                    PageName = o.PageName,
                    PageUrl = o.PageUrl,
                    PageIcon = o.PageIcon,
                    icon = o.PageIcon,
                    ParentId = o.ParentId,
                    RoleId = i.RoleId
                })
                .ToList();

                pages.ForEach(x => x.Childrens = allPages.Where(c => c.ParentId == x.Id).Select(o => new RetuAdminPage()
                {
                    Id = o.Id,
                    PageName = o.PageName,
                    PageUrl = o.PageUrl,
                    PageIcon = o.PageIcon,
                    icon = o.PageIcon,
                    ParentId = o.ParentId,
                    RoleId = o.RoleId
                }).ToList());
                userInfo.pages = pages;

                int i = db.Updateable<UserInfo>().SetColumns(c => c.UpdateTime == t).
                //SetColumns(c => c.OpenId == YRHelper.get_md5_32(OpenId)).
                SetColumns(c => c.IsValid == 1).
                SetColumns(c => c.CurrentTimeStamp == convert_time_int_to10(t)).
                SetColumns(c => c.LoginErrorCount == 0).
                Where(c => c.HashAddress == UserName && c.ManageUserPassportId == userInfo.PassportId).
                ExecuteCommand();
                if (i < 1)
                {
                    info.code = 400;
                    info.msg = "Network Exception";
                    return info;
                }
                //userInfo.OpenId = YRHelper.get_md5_32(OpenId);
                info.code = 200;
                info.msg = "success";
                info.data = userInfo;
            }

            return info;
        }

        #region 商户
        /// <summary>
        /// 商户列表
        /// </summary>
        /// <param name="OpenId">OpenId</param>
        /// <param name="UserName">用户名</param>
        /// <param name="IsValid">(0无效 1有效)</param>
        /// <param name="AddTime1">注册时间1</param>
        /// <param name="AddTime2">注册时间2</param>
        /// <param name="PageIndex">当前页</param>
        /// <param name="PageSize">每页数量</param>
        /// <returns></returns>
        [HttpGet]
        public MsgInfo<List<RetuAdminUser>> GetAdminUser(string OpenId, string UserName, string IsValid, 
            string AddTime1, string AddTime2, int PageIndex, int PageSize = 20)
        {
            MsgInfo<List<RetuAdminUser>> info = new();
            long _PassportId = IsLogin(OpenId);
            if (_PassportId <= 0)
            {
                info.code = 400;
                info.msg = "Please login";
                return info;
            }
            DateTime t1 = DateTime.Now;
            DateTime.TryParse(AddTime1, out t1);
            DateTime t2 = DateTime.Now;
            DateTime.TryParse(AddTime2, out t2);
            int _IsValid = 1;
            int.TryParse(IsValid, out _IsValid);

            int RoleId = GetRoleId(_PassportId);
            List<RetuAdminUser> list = GetMyAdminChilds<RetuAdminUser>( _PassportId,"", true);
            if (RoleId == 1)
            {
                list = db.Queryable<AdminUser>().LeftJoin<AdminRole>((c, d) => c.RoleId == d.Id).Select((c, d) => new RetuAdminUser
                {
                    Id = c.Id,
                    AddTime = c.AddTime,
                    UpdateTime = c.UpdateTime,
                    IsValid = c.IsValid,
                    Sort = c.Sort,
                    Ip = c.Ip,

                    PassportId = c.PassportId,
                    HashAddress = c.HashAddress,
                    UserName = c.UserName,
                    Pwd = c.Pwd,
                    RoleId = c.RoleId,
                    RoleName = d.RoleName,
                    ParentPassportId = c.ParentPassportId,
                    OpenId = c.OpenId,
                    CurrentTimeStamp = c.CurrentTimeStamp,
                    LoginErrorCount = c.LoginErrorCount,
                }).ToList();
            }
            list = (from c in list where c.ParentPassportId!=0 
                    select new RetuAdminUser()
                    {
                        Id = c.Id,
                        AddTime = c.AddTime,
                        UpdateTime = c.UpdateTime,
                        IsValid = c.IsValid,
                        Sort = c.Sort,
                        Ip = c.Ip,

                        PassportId = c.PassportId,
                        HashAddress = c.HashAddress,
                        UserName = c.UserName,
                        Pwd = c.Pwd,
                        RoleId = c.RoleId,
                        RoleName = c.RoleName,
                        ParentPassportId = c.ParentPassportId,
                        OpenId = c.OpenId,
                        CurrentTimeStamp = c.CurrentTimeStamp,
                        LoginErrorCount = c.LoginErrorCount,
                        GameAgentDetailsIds = string.Join(",", db.Queryable<AgentDetails>().Where(d => d.PassportId == c.PassportId).Select(c => c.AgentId).ToList()),
                        GameDetailsIds = string.Join(",", db.Queryable<GameDetails>().Where(d => d.ManageUserPassportId == c.PassportId).Select(c => c.GameTypeId).ToList()),
                        CurrentAgentCount = GetMyChildsAllT<UserInfo>("UserInfo", " and roleid=4 ", c.PassportId, "1")?.Count.ToString(),
                        CurrentMemberCount = GetMyChildsAllT<UserInfo>("UserInfo", "", c.PassportId, "1")?.Count.ToString(),
                    }).ToList();

            if (!string.IsNullOrWhiteSpace(UserName))
            {
                list = (from c in list where c.UserName == UserName select c).ToList();
            }
            //时间
            if (!string.IsNullOrWhiteSpace(AddTime1) && !string.IsNullOrWhiteSpace(AddTime2))
            {
                list = (from c in list where c.AddTime >= t1 && c.AddTime <= t2 select c).ToList();
            }
            else if (!string.IsNullOrWhiteSpace(AddTime1))
            {
                list = (from c in list where c.AddTime >= t1 select c).ToList();
            }
            else if (!string.IsNullOrWhiteSpace(AddTime2))
            {
                list = (from c in list where c.AddTime <= t2 select c).ToList();
            }
            if (!string.IsNullOrWhiteSpace(IsValid))
            {
                list = (from c in list where c.IsValid == _IsValid select c).ToList();
            }

            info.data_count = list.Count;
            if (PageIndex > 0)
            {
                list = (from c in list orderby c.AddTime descending select c).Skip((PageIndex - 1) * PageSize).Take(PageSize).ToList();
            }
            list = list.Count > 0 ? list : db.Queryable<AdminUser>().Where(x => x.OpenId == OpenId).Select(c => new RetuAdminUser()
            {
                Id = c.Id,
                AddTime = c.AddTime,
                UpdateTime = c.UpdateTime,
                IsValid = c.IsValid,
                Sort = c.Sort,
                Ip = c.Ip,

                PassportId = c.PassportId,
                HashAddress = c.HashAddress,
                UserName = c.UserName,
                Pwd = c.Pwd,
                RoleId = c.RoleId,
                ParentPassportId = c.ParentPassportId,
                OpenId = c.OpenId,
                CurrentTimeStamp = c.CurrentTimeStamp,
                LoginErrorCount = c.LoginErrorCount,
            }).ToList();
            info.code = 200;
            info.msg = "success";
            info.data = list;
            return info;
        }

        /// <summary>
        /// 添加/编辑商户(添加ManageId为空,修改ManageId不为空)
        /// </summary>
        /// <param name="param">
        /// {"OpenId":"9e26f6e2244f7e34ed8b4eabdcb2f9c4","IsValid":"(0无效 1有效)","UserName":"ls","HashAddress":"TEPXsez4NxKsPLoyoB5msPjF1j1Cr1pmbf",
        /// "Pwd":"123456","RoleId":1,"GameIds":"1,2","AgentIds":"1,2","ManageId":""}
        /// </param>
        /// <returns></returns>
        [HttpPost]
        public MsgInfo<string> PostManageUser(dynamic param)
        {
            MsgInfo<string> info = new();
            DateTime t = DateTime.Now;
            var data = JsonConvert.DeserializeObject<dynamic>(param.ToString());
            string OpenId = data.OpenId.ToString();
            if (Object.ReferenceEquals(null, data))
            {
                info.code = 400;
                info.msg = "Network Exception";
                return info;
            }
            long CurrentPassportId = IsLogin(OpenId);
            if (CurrentPassportId <= 0)
            {
                info.code = 400;
                info.msg = "Please login";
                return info;
            }
            string HashAddress = data.HashAddress.ToString();
            string UserName = data.UserName.ToString();
            string ManageId = data.ManageId.ToString();
            string Pwd = string.Empty;
            if (string.IsNullOrWhiteSpace(ManageId))
            {
                Pwd = YRHelper.get_md5_32(data.Pwd.ToString());
            }
            int RoleId = Convert.ToInt32(data.RoleId.ToString());
            string GameIds = data.GameIds.ToString();
            string AgentIds = data.AgentIds.ToString();
            string IsValid = data.IsValid.ToString();
            int _IsValid = 1;
            int.TryParse(IsValid, out _IsValid);
            int _ManageId = 0;
            int.TryParse(ManageId, out _ManageId);
            int RoleType = db.Queryable<AdminRole>().First(c => c.ManageUserPassportId == CurrentPassportId).RoleType;
            if (RoleType>2)
            {
                info.code = 400;
                info.msg = "No permission";
                return info;
            }

            int[] IntGameIds = Array.ConvertAll<string, int>(GameIds.Split(new char[] { ',' }, StringSplitOptions.RemoveEmptyEntries), s => int.Parse(s));
            int[] IntAgentIds = Array.ConvertAll<string, int>(AgentIds.Split(new char[] { ',' }, StringSplitOptions.RemoveEmptyEntries), s => int.Parse(s));
            if (string.IsNullOrWhiteSpace(ManageId))
            {
                string _OpenId = CreateOpenId();
                var random = new Random(Guid.NewGuid().GetHashCode());
                long _PassportId = random.Next(100000000, 999999999);

                var user = db.Queryable<AdminUser>().First(c => c.UserName == UserName);
                if (user != null)
                {
                    info.code = 400;
                    info.msg = "Account already exists";
                    return info;
                }
                long AdminPassportId = db.Queryable<AdminUser>().First(c => c.PassportId == CurrentPassportId).AdminPassportId;

                #region 添加页面/角色/权限
                Passport passport = new()
                {
                    AddTime = t,
                    UpdateTime = t,
                    IsValid = 1,
                    Sort = 1,
                    Ip = YRHelper.GetClientIPAddress(HttpContext),

                    PassportId = _PassportId
                };
                int m = db.Insertable(passport).ExecuteCommand();
                if (m < 1)
                {
                    info.code = 400;
                    info.msg = "Network Exception";
                    return info;
                }

                AdminRole adminRole = new AdminRole() {
                    AddTime = t,
                    UpdateTime = t,
                    IsValid = _IsValid,
                    Sort = 1,
                    Ip = YRHelper.GetClientIPAddress(HttpContext),

                    RoleType = RoleId,
                    RoleName= RoleId==3? "商户管理员":"庄家管理员",
                    RoleDetails= RoleId == 3 ? "商户管理员" : "庄家管理员",
                    ManageUserPassportId=_PassportId
                };
                int re_m = db.Insertable(adminRole).ExecuteReturnIdentity();
                if (re_m < 1)
                {
                    info.code = 400;
                    info.msg = "Network Exception";
                    return info;
                }


                AdminUser admin = new()
                {
                    AddTime = t,
                    UpdateTime = t,
                    IsValid = _IsValid,
                    Sort = 1,
                    Ip = YRHelper.GetClientIPAddress(HttpContext),

                    PassportId = _PassportId,
                    HashAddress = HashAddress,
                    UserName = UserName,
                    Pwd = Pwd,
                    RoleId = re_m,
                    ParentPassportId = CurrentPassportId,
                    AdminPassportId = _PassportId,
                    DominPassportId = RoleId == 2 ? _PassportId : AdminPassportId,
                    OpenId = YRHelper.get_md5_32(_OpenId),
                    CurrentTimeStamp = convert_time_int_to10(t),
                    GameDetailsIds = "",
                    IsGoogle = 0
                };
                int i = db.Insertable(admin).ExecuteCommand();
                if (i < 1)
                {
                    info.code = 400;
                    info.msg = "Network Exception";
                    return info;
                }

                string sql_select = @"
                    select distinct b.* from AdminPower a left join AdminPage b on a.PageId= b.Id and a.ManageUserPassportId = b.ManageUserPassportId
                     where a.ManageUserPassportId=@PassportId and a.RoleId=@RoleId
            ";
                SugarParameter[] sugars = new SugarParameter[] {
                new SugarParameter("@PassportId","999999999"),
                new SugarParameter("@RoleId",RoleId)
            };
                List<AdminPage> admins = db.Ado.SqlQuery<AdminPage>(sql_select, sugars);//比db.SqlQueryable兼容性更强，支持复杂SQL存储过程，缺点没有自带的分页操作

                List < AdminPage > adminPages = new List<AdminPage>();
                foreach (var item in admins)
                {
                    if (item.Id > 0)
                    {
                        AdminPage page = new AdminPage()
                        {
                            Id = item.Id,
                            AddTime = t,
                            UpdateTime = t,
                            IsValid = _IsValid,
                            Sort = 1,
                            Ip = YRHelper.GetClientIPAddress(HttpContext),

                            PageName = item.PageName,
                            PageUrl = item.PageUrl,
                            PageIcon = item.PageIcon,
                            ParentId = item.ParentId,
                            ManageUserPassportId = _PassportId
                        };
                        adminPages.Add(page);
                    }
                }
                db.Insertable(adminPages).ExecuteCommand();

                List<AdminPower> powers = db.Queryable<AdminPower>().Where(c=>c.ManageUserPassportId== 999999999 && c.RoleId==RoleId).ToList();
                List<AdminPower> adminPowers = new List<AdminPower>();
                foreach (var item in powers)
                {
                    AdminPower power = new AdminPower()
                    {
                        AddTime = t,
                        UpdateTime = t,
                        IsValid = _IsValid,
                        Sort = 1,
                        Ip = YRHelper.GetClientIPAddress(HttpContext),

                        RoleId = re_m,
                        PageId = item.PageId,
                        ManageUserPassportId = _PassportId
                    };
                    adminPowers.Add(power);
                }
                db.Insertable(adminPowers).ExecuteCommand();
                #endregion

                #region 添加代理详情
                //代理详情
                List<AgentDetails> agentDetails = new List<AgentDetails>();
                for (int k = 0; k < IntAgentIds.Length; k++)
                {
                    int AgentId = IntAgentIds[k];
                    AgentDetails details = new()
                    {
                        AddTime = t,
                        UpdateTime = t,
                        IsValid = 0,
                        Sort = 1,
                        Ip = YRHelper.GetClientIPAddress(HttpContext),

                        PassportId = _PassportId,
                        AgentId = AgentId
                    };
                    agentDetails.Add(details);
                }
                if (agentDetails.Count > 0)
                {
                    db.Insertable(agentDetails).ExecuteCommand();
                }
                #endregion

                #region 添加游戏详情
                //游戏详情
                List<GameDetails> addGameDetails = new List<GameDetails>();
                for (int k = 0; k < IntGameIds.Length; k++)
                {
                    var GameTypeId = IntGameIds[k];
                    GameDetails details = new()
                    {
                        AddTime = t,
                        UpdateTime = t,
                        IsValid = 1,
                        Sort = 1,
                        Ip = YRHelper.GetClientIPAddress(HttpContext),

                        GameTypeId = GameTypeId,
                        GameName = "",
                        Odds = 0,
                        Odds0 = 0,
                        Odds1 = 0,
                        Odds2 = 0,
                        Odds3 = 0,
                        Odds4 = 0,
                        Odds5 = 0,
                        Odds6 = 0,
                        Odds7 = 0,
                        Odds8 = 0,
                        Odds9 = 0,
                        GameFee = 0,
                        InvalidGameFee = 0,
                        MinQuota = 0,
                        MaxQuota = 0,
                        GameDetsils = "",
                        ManageUserPassportId = _PassportId,
                    };
                    addGameDetails.Add(details);
                }
                if (addGameDetails.Count > 0)
                {
                    db.Insertable(addGameDetails).ExecuteCommand();
                }
                #endregion

                List<AdminGameRelation> adminGames = new List<AdminGameRelation>();
                for (int k = 0; k < IntGameIds.Length; k++)
                {
                    int GameTypeId = IntGameIds[k];
                    AdminGameRelation adminGame = new()
                    {
                        AddTime = t,
                        UpdateTime = t,
                        IsValid = 1,
                        Sort = 1,
                        Ip = YRHelper.GetClientIPAddress(HttpContext),

                        PassportId = _PassportId,
                        GameDetailsId = GameTypeId,
                        HashAddress = "",
                        CoinType = -1
                    };
                    adminGames.Add(adminGame);
                }
                if (adminGames.Count > 0)
                {
                    db.Insertable(adminGames).ExecuteCommand();
                }
            }
            else
            {
                #region 游戏
                {
                    AdminUser adminUser = db.Queryable<AdminUser>().First(c => c.Id == _ManageId);
                    int[] gameRelationList = db.Queryable<GameDetails>().
                        Where(c => c.ManageUserPassportId == adminUser.PassportId).Select(c => c.GameTypeId).ToArray();

                    //需要删除的数据
                    int[] relationDel = gameRelationList.Except(IntGameIds).ToArray();
                    List<GameDetails> list = new List<GameDetails>();
                    for (int k = 0; k < relationDel.Length; k++)
                    {
                        int GameTypeId = relationDel[k];
                        int j = db.Deleteable<GameDetails>().
                            Where(c => c.ManageUserPassportId == adminUser.PassportId && c.GameTypeId == GameTypeId).ExecuteCommand();
                        if (j < 1)
                        {
                            info.code = 400;
                            info.msg = "Network Exception";
                            return info;
                        }
                    }


                    list.Clear();
                    //需要新增的数据
                    int[] relationAdd = IntGameIds.Except(gameRelationList).ToArray();
                    for (int k = 0; k < relationAdd.Length; k++)
                    {
                        int GameTypeId = relationAdd[k];
                        GameDetails adminGame = new()
                        {
                            AddTime = t,
                            UpdateTime = t,
                            IsValid = 1,
                            Sort = 1,
                            Ip = YRHelper.GetClientIPAddress(HttpContext),

                            GameTypeId = GameTypeId,
                            GameName = "",
                            Odds = 0,
                            Odds0 = 0,
                            Odds1 = 0,
                            Odds2 = 0,
                            Odds3 = 0,
                            Odds4 = 0,
                            Odds5 = 0,
                            Odds6 = 0,
                            Odds7 = 0,
                            Odds8 = 0,
                            Odds9 = 0,
                            GameFee = 0,
                            InvalidGameFee = 0,
                            MinQuota = 0,
                            MaxQuota = 0,
                            GameDetsils = "",
                            ManageUserPassportId = adminUser.PassportId,
                        };

                        list.Add(adminGame);
                    }
                    if (list.Count > 0)
                    {
                        int a = db.Insertable(list).ExecuteCommand();
                        if (a < 1)
                        {
                            info.code = 400;
                            info.msg = "Network Exception";
                            return info;
                        }
                    }
                }
                #endregion

                #region 代理
                {
                    AdminUser adminUser = db.Queryable<AdminUser>().First(c => c.Id == _ManageId);
                    //int[] IntAgentDetailsIds = Array.ConvertAll<string, int>(AgentIds.Split(new char[] { ',' }, StringSplitOptions.RemoveEmptyEntries), s => int.Parse(s));
                    int[] AgentDetailsList = db.Queryable<AgentDetails>().
                        Where(c => c.PassportId == adminUser.PassportId).Select(c => c.AgentId).ToArray();

                    //需要删除的数据
                    int[] relationDel = AgentDetailsList.Except(IntAgentIds).ToArray();
                    List<AgentDetails> list = new List<AgentDetails>();
                    for (int k = 0; k < relationDel.Length; k++)
                    {
                        int AgentId = relationDel[k];
                        int j = db.Deleteable<AgentDetails>().
                            Where(c => c.PassportId == adminUser.PassportId && c.AgentId == AgentId).ExecuteCommand();
                        if (j < 1)
                        {
                            info.code = 400;
                            info.msg = "Network Exception";
                            return info;
                        }
                    }


                    list.Clear();
                    //需要新增的数据
                    int[] relationAdd = IntAgentIds.Except(AgentDetailsList).ToArray();
                    for (int k = 0; k < relationAdd.Length; k++)
                    {
                        int AgentId = relationAdd[k];
                        AgentDetails agentDetails = new()
                        {
                            AddTime = t,
                            UpdateTime = t,
                            IsValid = 0,
                            Sort = 1,
                            Ip = YRHelper.GetClientIPAddress(HttpContext),

                            PassportId = adminUser.PassportId,
                            AgentId = AgentId
                        };

                        list.Add(agentDetails);
                    }
                    if (list.Count > 0)
                    {
                        int a = db.Insertable(list).ExecuteCommand();
                        if (a < 1)
                        {
                            info.code = 400;
                            info.msg = "Network Exception";
                            return info;
                        }
                    }
                }
                #endregion

                int i = db.Updateable<AdminUser>().SetColumns(c => c.UpdateTime == t).
                    SetColumns(c => c.RoleId == RoleId).
                    SetColumns(c => c.HashAddress == HashAddress).
                    SetColumns(c => c.UserName == UserName).
                    SetColumns(c => c.IsValid == _IsValid).
                    Where(c => c.Id == _ManageId).ExecuteCommand();
                if (i < 1)
                {
                    info.code = 400;
                    info.msg = "Network Exception";
                    return info;
                }

            }
            info.code = 200;
            info.msg = "success";

            return info;
        }
        #endregion

        #region 单个商户管理员列表
        /// <summary>
        /// 单个商户管理员列表
        /// </summary>
        /// <param name="OpenId">OpenId</param>
        /// <param name="UserName">用户名</param>
        /// <param name="IsValid">(0无效 1有效)</param>
        /// <param name="AddTime1">注册时间1</param>
        /// <param name="AddTime2">注册时间2</param>
        /// <param name="PageIndex">当前页</param>
        /// <param name="PageSize">每页数量</param>
        /// <returns></returns>
        /// <returns></returns>
        [HttpGet]
        public MsgInfo<List<RetuAdminUser>> GetShopAdminUserList(string OpenId, string UserName, string IsValid, 
            string AddTime1, string AddTime2, int PageIndex, int PageSize = 20)
        {
            MsgInfo<List<RetuAdminUser>> info = new();
            long _PassportId = IsLogin(OpenId);
            if (_PassportId <= 0)
            {
                info.code = 400;
                info.msg = "Please login";
                return info;
            }
            DateTime t1 = DateTime.Now;
            DateTime.TryParse(AddTime1, out t1);
            DateTime t2 = DateTime.Now;
            DateTime.TryParse(AddTime2, out t2);
            int _IsValid = 1;
            int.TryParse(IsValid, out _IsValid);

            int RoleId = GetRoleId(_PassportId);
            if (RoleId == 1)
            {

            }
            List<RetuAdminUser> list = db.Queryable<AdminUser>().LeftJoin<AdminRole>((c, d) => c.RoleId == d.Id).
                Where(c => c.AdminPassportId == _PassportId).Select((c, d) => new RetuAdminUser
                {
                    Id = c.Id,
                    AddTime = c.AddTime,
                    UpdateTime = c.UpdateTime,
                    IsValid = c.IsValid,
                    Sort = c.Sort,
                    Ip = c.Ip,

                    PassportId = c.PassportId,
                    HashAddress = c.HashAddress,
                    UserName = c.UserName,
                    Pwd = c.Pwd,
                    RoleId = c.RoleId,
                    RoleName = d.RoleName,
                    ParentPassportId = c.ParentPassportId,
                    OpenId = c.OpenId,
                    CurrentTimeStamp = c.CurrentTimeStamp,
                    LoginErrorCount = c.LoginErrorCount,
                }).ToList();

            if (!string.IsNullOrWhiteSpace(UserName))
            {
                list = (from c in list where c.UserName == UserName select c).ToList();
            }
            //时间
            if (!string.IsNullOrWhiteSpace(AddTime1) && !string.IsNullOrWhiteSpace(AddTime2))
            {
                list = (from c in list where c.AddTime >= t1 && c.AddTime <= t2 select c).ToList();
            }
            else if (!string.IsNullOrWhiteSpace(AddTime1))
            {
                list = (from c in list where c.AddTime >= t1 select c).ToList();
            }
            else if (!string.IsNullOrWhiteSpace(AddTime2))
            {
                list = (from c in list where c.AddTime <= t2 select c).ToList();
            }
            if (!string.IsNullOrWhiteSpace(IsValid))
            {
                list = (from c in list where c.IsValid == _IsValid select c).ToList();
            }

            info.data_count = list.Count;
            if (PageIndex > 0)
            {
                list = (from c in list orderby c.AddTime descending select c).Skip((PageIndex - 1) * PageSize).Take(PageSize).ToList();
            }
            info.code = 200;
            info.msg = "success";
            info.data = list;
            return info;
        }

        /// <summary>
        /// 添加/编辑单个商户管理员(添加ManageId为空,修改ManageId不为空)
        /// </summary>
        /// <param name="param">
        /// {"OpenId":"9e26f6e2244f7e34ed8b4eabdcb2f9c4","IsValid":"(0无效 1有效)","UserName":"ls",
        /// "Pwd":"123456","RoleId":1,"ManageId":""}
        /// </param>
        /// <returns></returns>
        [HttpPost]
        public MsgInfo<string> PostShopAdminUserList(dynamic param)
        {
            MsgInfo<string> info = new();
            DateTime t = DateTime.Now;
            var data = JsonConvert.DeserializeObject<dynamic>(param.ToString());
            string OpenId = data.OpenId.ToString();
            if (Object.ReferenceEquals(null, data))
            {
                info.code = 400;
                info.msg = "Network Exception";
                return info;
            }
            long CurrentPassportId = IsLogin(OpenId);
            if (CurrentPassportId <= 0)
            {
                info.code = 400;
                info.msg = "Please login";
                return info;
            }

            string UserName = data.UserName.ToString();
            string ManageId = data.ManageId.ToString();
            string Pwd = string.Empty;
            if (string.IsNullOrWhiteSpace(ManageId))
            {
                Pwd = YRHelper.get_md5_32(data.Pwd.ToString());
            }
            int RoleId = Convert.ToInt32(data.RoleId.ToString());
            string IsValid = data.IsValid.ToString();
            int _IsValid = 1;
            int.TryParse(IsValid, out _IsValid);
            int _ManageId = 0;
            int.TryParse(ManageId, out _ManageId);

            if (string.IsNullOrWhiteSpace(ManageId))
            {
                string _OpenId = CreateOpenId();
                var random = new Random(Guid.NewGuid().GetHashCode());
                long _PassportId = random.Next(100000000, 999999999);

                var user = db.Queryable<AdminUser>().First(c => c.UserName == UserName && c.AdminPassportId == CurrentPassportId);
                if (user != null)
                {
                    info.code = 400;
                    info.msg = "Account already exists";
                    return info;
                }


                Passport passport = new()
                {
                    AddTime = t,
                    UpdateTime = t,
                    IsValid = 1,
                    Sort = 1,
                    Ip = YRHelper.GetClientIPAddress(HttpContext),

                    PassportId = _PassportId
                };
                int m = db.Insertable(passport).ExecuteCommand();
                if (m < 1)
                {
                    info.code = 400;
                    info.msg = "Network Exception";
                    return info;
                }
                long DominPassportId = db.Queryable<AdminUser>().First(c => c.PassportId == CurrentPassportId).DominPassportId;


                AdminUser admin = new()
                {
                    AddTime = t,
                    UpdateTime = t,
                    IsValid = _IsValid,
                    Sort = 1,
                    Ip = YRHelper.GetClientIPAddress(HttpContext),

                    PassportId = _PassportId,
                    HashAddress = "",
                    UserName = UserName,
                    Pwd = Pwd,
                    RoleId = RoleId,
                    ParentPassportId = 0,
                    AdminPassportId = CurrentPassportId,
                    DominPassportId = DominPassportId,
                    OpenId = YRHelper.get_md5_32(_OpenId),
                    CurrentTimeStamp = convert_time_int_to10(t),
                    GameDetailsIds = "",
                    IsGoogle = 0
                };
                int i = db.Insertable(admin).ExecuteCommand();
                if (i < 1)
                {
                    info.code = 400;
                    info.msg = "Network Exception";
                    return info;
                }
            }
            else
            {
                int i = db.Updateable<AdminUser>().SetColumns(c => c.UpdateTime == t).
                    SetColumns(c => c.RoleId == RoleId).
                    SetColumns(c => c.IsValid == _IsValid).
                    Where(c => c.Id == _ManageId).ExecuteCommand();
                if (i < 1)
                {
                    info.code = 400;
                    info.msg = "Network Exception";
                    return info;
                }

            }
            info.code = 200;
            info.msg = "success";

            return info;
        }
        #endregion

        #region 添加/编辑代理
        /// <summary>
        /// 添加/编辑代理[AgentId为空添加，有值修改]
        /// </summary>
        /// <param name="param">
        /// {"OpenId":"9e26f6e2244f7e34ed8b4eabdcb2f9c4","UserName":"zs","HashAddress":"TEPXsez4NxKsPLoyoB5msPjF1j1Cr1pmbf","Pwd":"123456","UserType":"1",
        /// "OtherMsg":{"MemberSource":"0网页，1app，2telegram，3whatapp，4line，5其他，6商户添加"},"AgentId":"","IsValid":"0无效 1有效","AgentTypeId":""}
        /// </param>
        /// <returns></returns>
        [HttpPost]
        public MsgInfo<string> PostAgentUser(dynamic param)
        {
            MsgInfo<string> info = new();
            DateTime t = DateTime.Now;
            var data = JsonConvert.DeserializeObject<dynamic>(param.ToString());
            string OpenId = data.OpenId.ToString();
            string UserType = data.UserType.ToString();
            long PassportId = IsLogin(OpenId, UserType);
            if (PassportId <= 0)
            {
                info.code = 400;
                info.msg = "Please login";
                return info;
            }

            string TJCode = CreateTJCode();
            if (Object.ReferenceEquals(null, data))
            {
                info.code = 400;
                info.msg = "Network Exception";
                return info;
            }

            Int64 ManageUserPassportId = PassportId;
            string UserName = data.UserName.ToString();
            string HashAddress = data.HashAddress.ToString();
            string OtherMsg = data.OtherMsg.ToString();
            string AgentId = data.AgentId.ToString();
            string IsValid = data.IsValid.ToString();
            string AgentTypeId = data.AgentTypeId.ToString();

            var random = new Random(Guid.NewGuid().GetHashCode());
            long _PassportId = random.Next(100000000, 999999999);
            int ParentId = 999999999;
            int RoleId = 4;//代理4，会员100
            int _AgentId = 0;
            int.TryParse(AgentId, out _AgentId);
            int _IsValid = 1;
            int.TryParse(IsValid, out _IsValid);
            int _AgentTypeId = 0;
            int.TryParse(AgentTypeId, out _AgentTypeId);

            if (string.IsNullOrWhiteSpace(AgentId))
            {
                string Pwd = YRHelper.get_md5_32(data.Pwd.ToString());
                UserInfo user = db.Queryable<UserInfo>().First(c => c.UserName == UserName || c.HashAddress == HashAddress);
                if (user != null)
                {
                    info.code = 400;
                    info.msg = "Account already exists";
                    return info;
                }
                UserInfo userInfo = new UserInfo()
                {
                    AddTime = t,
                    UpdateTime = t,
                    IsValid = _IsValid,
                    Sort = 1,
                    Ip = YRHelper.GetClientIPAddress(HttpContext),

                    PassportId = _PassportId,
                    HashAddress = HashAddress,
                    UserName = UserName,
                    Pwd = Pwd,
                    TJCode = TJCode,
                    ParentId = ParentId,
                    RoleId = RoleId,
                    OpenId = "",
                    TgChatId = "",
                    AgentTypeID = _AgentTypeId,
                    ManageUserPassportId = ManageUserPassportId,
                    LoginErrorCount = 0,
                    OtherMsg = OtherMsg
                };
                int i = db.Insertable(userInfo).ExecuteCommand();
                if (i < 1)
                {
                    info.code = 400;
                    info.msg = "Network Exception";
                    return info;
                }
            }
            else
            {
                int i = db.Updateable<UserInfo>().
                    SetColumns(c => c.UserName == UserName).
                    SetColumns(c => c.HashAddress == HashAddress).
                    SetColumns(c => c.IsValid == _IsValid).
                    SetColumns(c => c.AgentTypeID == _AgentTypeId).
                    SetColumns(c => c.OtherMsg == OtherMsg).
                    Where(c => c.Id == _AgentId).ExecuteCommand();
                if (i < 1)
                {
                    info.code = 400;
                    info.msg = "Network Exception";
                    return info;
                }
            }

            info.code = 200;
            info.msg = "success";
            return info;
        }

        /// <summary>
        /// 注册[后台添加顶级代理 默认"ParentTJCode":"",ManageUserPassportId默认当前账户所属的PassportId]
        /// </summary>
        /// <param name="param">
        /// {"UserName":"zs","TgChatId":"","HashAddress":"TEPXsez4NxKsPLoyoB5msPjF1j1Cr1pmbf","Pwd":"123456","ParentTJCode":"","ManageUserPassportId":0,
        /// "OtherMsg":{"TGName":"123","MemberSource":"0网页，1app，2telegram，3whatapp，4line，5其他，6商户添加"}}
        /// </param>
        /// <returns></returns>
        [HttpPost]
        public MsgInfo<UserInfo> Register(dynamic param)
        {
            //1.带注册码注册，先根据注册码获取到商户PassportId，将其赋值给ManageUserPassportId
            //2.无注册码注册，默认ManageUserPassportId为502423741
            //3.根据ManageUserPassportId获取代理类型
            //4.查看钱包地址、tgchatid、用户名是否在ManageUserPassportId已注册

            MsgInfo<UserInfo> info = new();
            var data = JsonConvert.DeserializeObject<dynamic>(param.ToString());
            DateTime t = DateTime.Now;

            string OpenId = CreateOpenId();
            string TJCode = CreateTJCode();

            if (Object.ReferenceEquals(null, data))
            {
                info.code = 400;
                info.msg = "Network Exception";
                return info;
            }

            Int64 ManageUserPassportId = 502423741;
            string UserName = data.UserName.ToString();
            string TgChatId = data.TgChatId.ToString();
            string Pwd = YRHelper.get_md5_32(data.Pwd.ToString());
            string HashAddress = data.HashAddress.ToString();
            string ParentTJCode = data.ParentTJCode.ToString();
            string OtherMsg = data.OtherMsg.ToString();
            var random = new Random(Guid.NewGuid().GetHashCode());
            long _PassportId = random.Next(100000000, 999999999);
            int ParentId = 999999999;
            int RoleId = 100;//会员100
            int AgentTypeID = 0;

            if (!string.IsNullOrWhiteSpace(data.ManageUserPassportId.ToString()) && data.ManageUserPassportId.ToString() != "0")
            {
                Int64.TryParse(data.ManageUserPassportId.ToString(), out ManageUserPassportId);
            }

            RetuOtherMsg retuOtherMsg = JsonConvert.DeserializeObject<RetuOtherMsg>(OtherMsg);
            if (retuOtherMsg == null || retuOtherMsg?.MemberSource == null)
            {
                info.code = 400;
                info.msg = "Network Exception";
                return info;
            }
            if (string.IsNullOrWhiteSpace(HashAddress))
            {
                info.code = 400;
                info.msg = "wallet dont empty";
                return info;
            }

            UserInfo user = new UserInfo();
            if (!string.IsNullOrWhiteSpace(TgChatId))
            {
                user = db.Queryable<UserInfo>().First(c => c.TgChatId == TgChatId && c.ManageUserPassportId == ManageUserPassportId);
                if (user != null)
                {
                    info.code = 400;
                    info.msg = "tgchaid already exists";
                    return info;
                }
            }

            if (!string.IsNullOrWhiteSpace(UserName))
            {
                user = db.Queryable<UserInfo>().First(c => c.UserName == UserName && c.ManageUserPassportId == ManageUserPassportId);
                if (user != null)
                {
                    info.code = 400;
                    info.msg = "Account already exists";
                    return info;
                }
            }

            user = db.Queryable<UserInfo>().First(c => c.HashAddress == HashAddress && c.ManageUserPassportId == ManageUserPassportId);
            if (user != null)
            {
                info.code = 400;
                info.msg = "Account already exists";
                return info;
            }

            if (!string.IsNullOrWhiteSpace(ParentTJCode))
            {
                user = db.Queryable<UserInfo>().First(c => c.TJCode == ParentTJCode);
                if (user != null)
                {
                    ParentId = (int)user.PassportId;
                    Int64.TryParse(user.ManageUserPassportId.ToString(), out ManageUserPassportId);
                }
            }

            //根据ManageUserPassportId获取代理类型
            if (ManageUserPassportId != 0)
            {
                RoleId = 4;
                AgentDetails agentDetails = db.Queryable<AgentDetails>().First(c => c.PassportId == ManageUserPassportId && c.IsValid == 1);
                AgentTypeID = agentDetails.AgentId;
            }

            Passport passport = new()
            {
                AddTime = t,
                UpdateTime = t,
                IsValid = 1,
                Sort = 1,
                Ip = YRHelper.GetClientIPAddress(HttpContext),

                PassportId = _PassportId
            };
            int m = db.Insertable(passport).ExecuteCommand();
            if (m < 1)
            {
                info.code = 400;
                info.msg = "Network Exception";
                return info;
            }

            UserInfo userInfo = new UserInfo()
            {
                AddTime = t,
                UpdateTime = t,
                IsValid = 1,
                Sort = 1,
                Ip = YRHelper.GetClientIPAddress(HttpContext),

                PassportId = _PassportId,
                HashAddress = HashAddress,
                UserName = UserName,
                Pwd = Pwd,
                TJCode = TJCode,
                ParentId = ParentId,
                RoleId = RoleId,
                OpenId = YRHelper.get_md5_32(OpenId),
                TgChatId = TgChatId,
                AgentTypeID = AgentTypeID,
                ManageUserPassportId = ManageUserPassportId,
                LoginErrorCount = 0,
                OtherMsg = OtherMsg
            };
            int i = db.Insertable(userInfo).ExecuteCommand();
            if (i < 1)
            {
                info.code = 400;
                info.msg = "Network Exception";
                return info;
            }

            info.code = 200;
            info.msg = "success";
            info.data = userInfo;
            return info;
        }
        #endregion


        /// <summary>
        /// 获取代理自身的下级代理及会员
        /// </summary>
        /// <param name="PassportId"></param>
        /// <returns></returns>
        private List<MyChild> GetMyChildByLevel(long PassportId)
        {
            string sql = $@"
                CREATE TABLE #userinfo
                (
	                username nvarchar(20),
	                ParentId int,
	                MyLevel INT,
	                PassportId bigint,
                    AgentTypeID INT,
					ManageUserPassportId bigint
                );
                WITH T 
                AS (SELECT 
			                username,
			                ParentId,
							1 AS mylevel,
			                PassportId,
                            AgentTypeID,
							ManageUserPassportId
	                FROM 
	                (SELECT username,ParentId,PassportId,AgentTypeID,ManageUserPassportId FROM UserInfo AS b where b.ParentId=@PassportId) AS c
	                UNION ALL
	                SELECT 
			                a.username,
			                a.ParentId,
			                b.mylevel + 1,
			                a.PassportId,
                            a.AgentTypeID,
							a.ManageUserPassportId
	                FROM UserInfo a
		                JOIN T b
			                ON a.ParentId = b.PassportId
			                )
			                INSERT INTO #userinfo
			                (
				                username,
								ParentId,
				                MyLevel,
				                PassportId,
                                AgentTypeID,
								ManageUserPassportId
			                ) SELECT * FROM T   ORDER BY T.mylevel DESC  OPTION (MAXRECURSION 32700);
                            SELECT * FROM #userinfo
                DROP TABLE #userinfo
            ";

            SugarParameter[] sugars = new SugarParameter[] {
                new SugarParameter("@PassportId",PassportId)
            };
            List<MyChild> mies = db.Ado.SqlQuery<MyChild>(sql, sugars);//比db.SqlQueryable兼容性更强，支持复杂SQL存储过程，缺点没有自带的分页操作

            return mies;
        }

        /// <summary>
        /// 获取我所有的下级(管理员的下级)
        /// </summary>
        /// <param name="_PassportId"></param>
        /// <param name="sqlwhere">where条件</param>
        /// <param name="IsContainsMe">是否包含自己</param>
        /// <returns></returns>
        [HttpGet]
        private List<T> GetMyAdminChilds<T>(Int64 _PassportId, string sqlwhere, bool IsContainsMe = true)
        {
            List<T> list = new List<T>();
                string sql = $@"
                CREATE TABLE #userinfo
                (
	                username nvarchar(20),
	                ParentPassportId int,
	                MyLevel INT,
	                PassportId bigint
                );
                WITH T 
                AS (SELECT 
			                username,
			                ParentPassportId,
							1 AS mylevel,
			                PassportId
	                FROM 
	                (SELECT username,ParentPassportId,PassportId FROM AdminUser AS b where b.ParentPassportId=@PassportId) AS c
	                UNION ALL
	                SELECT 
			                a.username,
			                a.ParentPassportId,
			                b.mylevel + 1,
			                a.PassportId
	                FROM AdminUser a
		                JOIN T b
			                ON a.ParentPassportId = b.PassportId 
			                )
			                INSERT INTO #userinfo
			                (
				                username,
								ParentPassportId,
				                MyLevel,
				                PassportId
			                ) SELECT * FROM T   ORDER BY T.mylevel DESC  OPTION (MAXRECURSION 32700);  ";
                if (IsContainsMe)
                {
                    sql += $@"
				select b.* from 
                (
                SELECT username, ParentPassportId, 0 mylevel, PassportId FROM AdminUser AS b WHERE b.PassportId = @PassportId
                UNION ALL
                SELECT * FROM 
                #userinfo
                )  a join AdminUser b on a.PassportId = b.PassportId where 1=1 {sqlwhere}
                DROP TABLE #userinfo 
            ";
                }
                else
                {
                    sql += $@"
                SELECT b.* FROM 
                #userinfo 
                a join UserInfo b on a.PassportId = b.PassportId where 1=1 {sqlwhere}
                DROP TABLE #userinfo 
            ";
                }


                SugarParameter[] sugars = new SugarParameter[] {
                new SugarParameter("@PassportId",_PassportId)
            };
                list = db.Ado.SqlQuery<T>(sql, sugars);//比db.SqlQueryable兼容性更强，支持复杂SQL存储过程，缺点没有自带的分页操作
            
            return list;

        }

        /// <summary>
        /// 获取我所有的下级(UserType:0为管理员用户，1为会员用户)
        /// </summary>
        /// <param name="_PassportId"></param>
        /// <param name="sqlwhere">where条件</param>
        /// <param name="UserType">0为管理员用户，1为会员用户</param>
        /// <param name="IsContainsMe">是否包含自己</param>
        /// <returns></returns>
        [HttpGet]
        private List<T> GetMyChilds<T>(Int64 _PassportId, string sqlwhere, string UserType, bool IsContainsMe = true)
        {
            List<T> list = new List<T>();
            if (UserType == "0")
            {
                string sql = $@"
                CREATE TABLE #userinfo
                (
	                username nvarchar(20),
	                ParentPassportId int,
	                MyLevel INT,
	                PassportId bigint
                );
                WITH T 
                AS (SELECT 
			                username,
			                ParentPassportId,
							1 AS mylevel,
			                PassportId
	                FROM 
	                (SELECT username,ParentPassportId,PassportId FROM AdminUser AS b where b.ParentPassportId=@PassportId) AS c
	                UNION ALL
	                SELECT 
			                a.username,
			                a.ParentPassportId,
			                b.mylevel + 1,
			                a.PassportId
	                FROM AdminUser a
		                JOIN T b
			                ON a.ParentPassportId = b.PassportId 
			                )
			                INSERT INTO #userinfo
			                (
				                username,
								ParentPassportId,
				                MyLevel,
				                PassportId
			                ) SELECT * FROM T   ORDER BY T.mylevel DESC  OPTION (MAXRECURSION 32700);  ";
                if (IsContainsMe)
                {
                    sql += $@"
				select b.* from 
                (
                SELECT username, ParentPassportId, 0 mylevel, PassportId FROM AdminUser AS b WHERE b.PassportId = @PassportId
                UNION ALL
                SELECT * FROM 
                #userinfo
                )  a join UserInfo b on a.PassportId = b.ManageUserPassportId where 1=1 {sqlwhere}
                DROP TABLE #userinfo 
            ";
                }
                else
                {
                    sql += $@"
                SELECT b.* FROM 
                #userinfo 
                a join UserInfo b on a.PassportId = b.ManageUserPassportId where 1=1 {sqlwhere}
                DROP TABLE #userinfo 
            ";
                }


                SugarParameter[] sugars = new SugarParameter[] {
                new SugarParameter("@PassportId",_PassportId)
            };
                list = db.Ado.SqlQuery<T>(sql, sugars);//比db.SqlQueryable兼容性更强，支持复杂SQL存储过程，缺点没有自带的分页操作
            }
            else if (UserType == "1")
            {
                string sql = $@"
                CREATE TABLE #userinfo
                (
	                username nvarchar(20),
	                ParentId int,
	                MyLevel INT,
	                PassportId bigint
                );
                WITH T 
                AS (SELECT 
			                username,
			                ParentId,
							1 AS mylevel,
			                PassportId
	                FROM 
	                (SELECT username,ParentId,PassportId FROM UserInfo AS b where b.ParentId=@PassportId) AS c
	                UNION ALL
	                SELECT 
			                a.username,
			                a.ParentId,
			                b.mylevel + 1,
			                a.PassportId
	                FROM UserInfo a
		                JOIN T b
			                ON a.ParentId = b.PassportId and a.ParentId=b.ParentId 
			                )
			                INSERT INTO #userinfo
			                (
				                username,
								ParentId,
				                MyLevel,
				                PassportId
			                ) SELECT * FROM T   ORDER BY T.mylevel DESC  OPTION (MAXRECURSION 32700);";

                if (IsContainsMe)
                {
                    sql += $@"select b.* from 
                (
                                SELECT username, ParentId, 0 mylevel, PassportId FROM UserInfo AS b WHERE b.PassportId = @PassportId
                                UNION ALL
                                SELECT * FROM 
							    #userinfo
                ) a
                join userinfo b on  a.PassportId = b.PassportId  where 1=1 {sqlwhere}
                DROP TABLE #userinfo 
            ";
                }
                else
                {
                    sql += $@"
                                SELECT b.* FROM 
							    #userinfo
                a
                join userinfo b on  a.PassportId = b.PassportId  where 1=1 {sqlwhere}
                DROP TABLE #userinfo 
            ";
                }


                SugarParameter[] sugars = new SugarParameter[] {
                new SugarParameter("@PassportId",_PassportId)
            };
                list = db.Ado.SqlQuery<T>(sql, sugars);//比db.SqlQueryable兼容性更强，支持复杂SQL存储过程，缺点没有自带的分页操作
            }

            return list;

        }

        /// <summary>
        /// 获取我所有的上级(UserType:0为管理员用户，1为会员用户)
        /// </summary>
        /// <param name="_PassportId">_PassportId(投注人)</param>
        /// <param name="UserType">0为管理员用户，1为会员用户</param>
        /// <returns></returns>
        [HttpGet]
        private List<MyParent> GetMyParent(Int64 _PassportId, string UserType)
        {
            List<MyParent> list = new List<MyParent>();

            if (UserType == "0")
            {
                string sql = @"
                CREATE TABLE #userinfo
                (
                    ParentId int,
                    Id int,
                    MyLevel INT,
                    HashAddress nvarchar(200),
                    PassportId bigint
                );
                WITH T 
                AS (SELECT 
                    ParentPassportId,
                    Id,1 AS mylevel,
					HashAddress,
                    PassportId
                    FROM 
                    (SELECT ParentPassportId,Id,HashAddress,PassportId FROM AdminUser AS b where b.PassportId=@ParentPassportId) AS c
                    UNION ALL
                    SELECT 
                    a.ParentPassportId,a.Id,
                    b.mylevel + 1,a.HashAddress,
                    a.PassportId
                    FROM AdminUser a
                    JOIN T b
                    ON a.PassportId = b.ParentPassportId 
                    )
                    INSERT INTO #userinfo
                    (
                    ParentId,
                    Id,
                    MyLevel,
                    HashAddress,
                    PassportId
                    ) SELECT * FROM T   ORDER BY T.mylevel DESC  OPTION (MAXRECURSION 32700);
                    select * from #userinfo where PassportId!=@ParentPassportId
                DROP TABLE #userinfo  
            ";

                SugarParameter[] sugars = new SugarParameter[] {
                new SugarParameter("@ParentPassportId",_PassportId)
            };
                list = db.Ado.SqlQuery<MyParent>(sql, sugars);//比db.SqlQueryable兼容性更强，支持复杂SQL存储过程，缺点没有自带的分页操作
            }
            else if (UserType == "1")
            {
                string sql = @"
                CREATE TABLE #userinfo
                (
                    ParentId int,
                    Id int,
                    MyLevel INT,
                    PassportId bigint,
                    HashAddress nvarchar(200),
                    ManageUserPassportId bigint
                );
                WITH T 
                AS (SELECT 
                    ParentId,
                    Id,1 AS mylevel,
                    PassportId,HashAddress,
					ManageUserPassportId
                    FROM 
                    (SELECT ParentId,Id,PassportId,HashAddress,ManageUserPassportId FROM UserInfo AS b where b.PassportId=@PassportId) AS c
                    UNION ALL
                    SELECT 
                    a.ParentId,a.Id,
                    b.mylevel + 1,
                    a.PassportId,a.HashAddress,a.ManageUserPassportId
                    FROM UserInfo a
                    JOIN T b
                    ON a.PassportId = b.ParentId 
                    )
                    INSERT INTO #userinfo
                    (
                    ParentId,
                    Id,
                    MyLevel,
                    PassportId,
                    HashAddress,
                    ManageUserPassportId
                    ) SELECT * FROM T   ORDER BY T.mylevel DESC  OPTION (MAXRECURSION 32700);
                    select * from #userinfo where PassportId!=@PassportId
                DROP TABLE #userinfo 
            ";

                SugarParameter[] sugars = new SugarParameter[] {
                new SugarParameter("@PassportId",_PassportId)
            };
                list = db.Ado.SqlQuery<MyParent>(sql, sugars);//比db.SqlQueryable兼容性更强，支持复杂SQL存储过程，缺点没有自带的分页操作

                #region 获取顶级代理
                var topone = (from c in list where c.ParentId == 999999999 select c).FirstOrDefault();
                if (topone == null)
                {
                    return null;
                }
                var ManageUserPassportId = topone.ManageUserPassportId;
                var Level = topone.MyLevel;
                string sql_manage = @"
                CREATE TABLE #userinfo
                (
                    ParentId int,
                    Id int,
                    MyLevel INT,
                    PassportId bigint
                );
                WITH T 
                AS (SELECT 
                    ParentPassportId,
                    Id,@Level AS mylevel,
                    PassportId
                    FROM 
                    (SELECT ParentPassportId,Id,PassportId FROM AdminUser AS b where b.PassportId=@ParentPassportId) AS c
                    UNION ALL
                    SELECT 
                    a.ParentPassportId,a.Id,
                    b.mylevel + 1,
                    a.PassportId
                    FROM AdminUser a
                    JOIN T b
                    ON a.PassportId = b.ParentPassportId 
                    )
                    INSERT INTO #userinfo
                    (
                    ParentId,
                    Id,
                    MyLevel,
                    PassportId
                    ) SELECT * FROM T   ORDER BY T.mylevel DESC  OPTION (MAXRECURSION 32700);
                    select * from #userinfo
                DROP TABLE #userinfo  
            ";

                SugarParameter[] sugars_manage = new SugarParameter[] {
                    new SugarParameter("@Level",Level),
                    new SugarParameter("@ParentPassportId",ManageUserPassportId)
                };
                List<MyParent> list_manage = db.Ado.SqlQuery<MyParent>(sql_manage, sugars_manage);//比db.SqlQueryable兼容性更强，支持复杂SQL存储过程，缺点没有自带的分页操作
                #endregion
                list.AddRange(list_manage);
            }
            return list;
        }

        /// <summary>
        /// 获取我所有下级的xx数据
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="tableName"></param>
        /// <param name="sqlWhere"></param>
        /// <param name="_PassportId"></param>
        /// <param name="UserType"></param>
        /// <param name="IsContainsMe">是否包含自己</param>
        /// <returns></returns>
        private List<T> GetMyChildsAllT<T>(string tableName, string sqlWhere, Int64 _PassportId, string UserType, bool IsContainsMe = true)
        {
            List<T> list = new List<T>();

            string tablePassportId = " OR b.ManageUserPassportId = a.PassportId ";
            if (tableName.ToUpper() == "ADMINUSER")
            {
                tablePassportId = " OR b.ParentPassportId = a.PassportId ";
            }

            string tableManagePassportId = "PassportId";
            if (UserType == "1" && tableName.ToUpper() == "USERINFO")
            {
                tableManagePassportId = "ManageUserPassportId";
            }

            if (UserType == "0")
            {
                string sql = $@"
                CREATE TABLE #userinfo
                (
	                username nvarchar(20),
	                ParentPassportId int,
	                MyLevel INT,
	                PassportId bigint
                );
                WITH T 
                AS (SELECT 
			                username,
			                ParentPassportId,
							1 AS mylevel,
			                PassportId
	                FROM 
	                (SELECT username,ParentPassportId,PassportId FROM AdminUser AS b where b.ParentPassportId=@ManageUserPassportId) AS c
	                UNION ALL
	                SELECT 
			                a.username,
			                a.ParentPassportId,
			                b.mylevel + 1,
			                a.PassportId
	                FROM AdminUser a
		                JOIN T b
			                ON a.ParentPassportId = b.PassportId 
			                )
			                INSERT INTO #userinfo
			                (
				                username,
								ParentPassportId,
				                MyLevel,
				                PassportId
			                ) SELECT * FROM T   ORDER BY T.mylevel DESC  OPTION (MAXRECURSION 32700);
                            {(IsContainsMe ?
                                $@"select b.* from 
                                (
                                SELECT username, ParentPassportId, 0 mylevel, PassportId FROM AdminUser AS b WHERE b.PassportId = @ManageUserPassportId
                                UNION ALL
                                SELECT * FROM 
							    #userinfo
                                )  a
							    JOIN {tableName} b ON a.PassportId=b.PassportId {tablePassportId} WHERE 1=1 {sqlWhere}" :
                                $@"
                                SELECT b.* FROM #userinfo a JOIN {tableName} b ON a.PassportId=b.PassportId {tablePassportId} 
                                WHERE 1=1 {sqlWhere}
                                "
                                )}			                
                DROP TABLE #userinfo 
            ";

                SugarParameter[] sugars = new SugarParameter[] {
                new SugarParameter("@ManageUserPassportId",_PassportId)
            };
                list = db.Ado.SqlQuery<T>(sql, sugars);//比db.SqlQueryable兼容性更强，支持复杂SQL存储过程，缺点没有自带的分页操作
            }
            else if (UserType == "1")
            {
                string sql = $@"
                CREATE TABLE #userinfo
                (
	                username nvarchar(20),
	                ParentId int,
	                MyLevel INT,
	                PassportId bigint
                );
                WITH T 
                AS (SELECT 
			                username,
			                ParentId,
							1 AS mylevel,
			                PassportId
	                FROM 
	                (SELECT username,ParentId,PassportId FROM UserInfo AS b where b.ParentId=@PassportId) AS c
	                UNION ALL
	                SELECT 
			                a.username,
			                a.ParentId,
			                b.mylevel + 1,
			                a.PassportId
	                FROM UserInfo a
		                JOIN T b
			                ON a.ParentId = b.PassportId and a.ParentId=b.ParentId 
			                )
			                INSERT INTO #userinfo
			                (
				                username,
								ParentId,
				                MyLevel,
				                PassportId
			                ) SELECT * FROM T   ORDER BY T.mylevel DESC  OPTION (MAXRECURSION 32700);
                            {(IsContainsMe ?
                                $@"select b.* from 
                                (
                                SELECT username, ParentId, 0 mylevel, PassportId FROM UserInfo AS b WHERE b.{tableManagePassportId} = @PassportId
                                UNION ALL
                                SELECT * FROM 
							    #userinfo
                                )  a
							    JOIN {tableName} b ON a.PassportId=b.PassportId WHERE 1=1 {sqlWhere}" :
                                $@"
                                SELECT b.* FROM #userinfo a JOIN {tableName} b ON a.PassportId=b.{tableManagePassportId} WHERE 1=1 {sqlWhere}
                                "
                                )}
                DROP TABLE #userinfo 
            ";

                SugarParameter[] sugars = new SugarParameter[] {
                new SugarParameter("@PassportId",_PassportId)
            };
                list = db.Ado.SqlQuery<T>(sql, sugars);//比db.SqlQueryable兼容性更强，支持复杂SQL存储过程，缺点没有自带的分页操作
            }

            return list;
        }


        /// <summary>
        /// 获取我所有下级的xx数据(带等级数据列)
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="tableName"></param>
        /// <param name="sqlWhere"></param>
        /// <param name="_PassportId"></param>
        /// <param name="UserType"></param>
        /// <param name="IsContainsMe">是否包含自己</param>
        /// <returns></returns>
        private List<T> GetMyChildsAllLevelT<T>(string tableName, string sqlWhere, Int64 _PassportId, string UserType, bool IsContainsMe = true)
        {
            List<T> list = new List<T>();

            string tablePassportId = " OR b.ManageUserPassportId = a.PassportId ";
            if (tableName.ToUpper() == "ADMINUSER")
            {
                tablePassportId = " OR b.ParentPassportId = a.PassportId ";
            }

            string tableManagePassportId = "PassportId";
            if (UserType == "1" && tableName.ToUpper() == "USERINFO")
            {
                tableManagePassportId = "ManageUserPassportId";
            }

            if (UserType == "0")
            {
                string sql = $@"
                CREATE TABLE #userinfo
                (
	                username nvarchar(20),
	                ParentPassportId int,
	                MyLevel INT,
	                PassportId bigint
                );
                WITH T 
                AS (SELECT 
			                username,
			                ParentPassportId,
							1 AS mylevel,
			                PassportId
	                FROM 
	                (SELECT username,ParentPassportId,PassportId FROM AdminUser AS b where b.ParentPassportId=@ManageUserPassportId) AS c
	                UNION ALL
	                SELECT 
			                a.username,
			                a.ParentPassportId,
			                b.mylevel + 1,
			                a.PassportId
	                FROM AdminUser a
		                JOIN T b
			                ON a.ParentPassportId = b.PassportId 
			                )
			                INSERT INTO #userinfo
			                (
				                username,
								ParentPassportId,
				                MyLevel,
				                PassportId
			                ) SELECT * FROM T   ORDER BY T.mylevel DESC  OPTION (MAXRECURSION 32700);
                            {(IsContainsMe ?
                                $@"select b.*,a.MyLevel,a.username from 
                                (
                                SELECT username, ParentPassportId, 0 mylevel, PassportId FROM AdminUser AS b WHERE b.PassportId = @ManageUserPassportId
                                UNION ALL
                                SELECT * FROM 
							    #userinfo
                                )  a
							    JOIN {tableName} b ON a.PassportId=b.PassportId {tablePassportId} WHERE 1=1 {sqlWhere}" :
                                $@"
                                SELECT b.*,a.MyLevel,a.username FROM #userinfo a JOIN {tableName} b ON a.PassportId=b.PassportId {tablePassportId} 
                                WHERE 1=1 {sqlWhere}
                                "
                                )}			                
                DROP TABLE #userinfo 
            ";

                SugarParameter[] sugars = new SugarParameter[] {
                new SugarParameter("@ManageUserPassportId",_PassportId)
            };
                list = db.Ado.SqlQuery<T>(sql, sugars);//比db.SqlQueryable兼容性更强，支持复杂SQL存储过程，缺点没有自带的分页操作
            }
            else if (UserType == "1")
            {
                string sql = $@"
                CREATE TABLE #userinfo
                (
	                username nvarchar(20),
	                ParentId int,
	                MyLevel INT,
	                PassportId bigint
                );
                WITH T 
                AS (SELECT 
			                username,
			                ParentId,
							1 AS mylevel,
			                PassportId
	                FROM 
	                (SELECT username,ParentId,PassportId FROM UserInfo AS b where b.ParentId=@PassportId) AS c
	                UNION ALL
	                SELECT 
			                a.username,
			                a.ParentId,
			                b.mylevel + 1,
			                a.PassportId
	                FROM UserInfo a
		                JOIN T b
			                ON a.ParentId = b.PassportId and a.ParentId=b.ParentId 
			                )
			                INSERT INTO #userinfo
			                (
				                username,
								ParentId,
				                MyLevel,
				                PassportId
			                ) SELECT * FROM T   ORDER BY T.mylevel DESC  OPTION (MAXRECURSION 32700);
                            {(IsContainsMe ?
                                $@"select b.*,a.MyLevel,a.username from 
                                (
                                SELECT username, ParentId, 0 mylevel, PassportId FROM UserInfo AS b WHERE b.{tableManagePassportId} = @PassportId
                                UNION ALL
                                SELECT * FROM 
							    #userinfo
                                )  a
							    JOIN {tableName} b ON a.PassportId=b.PassportId WHERE 1=1 {sqlWhere}" :
                                $@"
                                SELECT b.*,a.MyLevel,a.username FROM #userinfo a JOIN {tableName} b ON a.PassportId=b.{tableManagePassportId} WHERE 1=1 {sqlWhere}
                                "
                                )}
                DROP TABLE #userinfo 
            ";

                SugarParameter[] sugars = new SugarParameter[] {
                new SugarParameter("@PassportId",_PassportId)
            };
                list = db.Ado.SqlQuery<T>(sql, sugars);//比db.SqlQueryable兼容性更强，支持复杂SQL存储过程，缺点没有自带的分页操作
            }

            return list;
        }

        /// <summary>
        /// 获取所有子投注（包含自己）
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="_PassportId"></param>
        /// <param name="UserType"></param>
        /// <returns></returns>
        private List<T> GetMyChildsBet<T>(Int64 _PassportId, string UserType)
        {
            List<T> list = new List<T>();

            if (UserType == "0")
            {
                string sql = $@"
                CREATE TABLE #userinfo
                (
	                username nvarchar(20),
	                ParentPassportId int,
	                MyLevel INT,
	                PassportId bigint
                );
                WITH T 
                AS (SELECT 
			                username,
			                ParentPassportId,
							1 AS mylevel,
			                PassportId
	                FROM 
	                (SELECT username,ParentPassportId,PassportId FROM AdminUser AS b where b.ParentPassportId=@PassportId) AS c
	                UNION ALL
	                SELECT 
			                a.username,
			                a.ParentPassportId,
			                b.mylevel + 1,
			                a.PassportId
	                FROM AdminUser a
		                JOIN T b
			                ON a.ParentPassportId = b.PassportId 
			                )
			                INSERT INTO #userinfo
			                (
				                username,
								ParentPassportId,
				                MyLevel,
				                PassportId
			                ) SELECT * FROM T   ORDER BY T.mylevel DESC  OPTION (MAXRECURSION 32700);     
                select *,(VaildBetAmount*GameFee/100) gamefeeAmount,(UnVaildBetAmount*InvalidGameFee/100) unvaildbetfeeAmount,
                (select top 1 product_ref from RechargeCashOrder m where replace(m.OrderID,'C','')=replace(betlist.OrderID,'BET','')) as cash_block_hash,
				case mylevel
                when 0 then 'Company'
				when 1 then 'Company'
				else (select top 1 UserName from UserInfo where ParentId=betlist.PassportId)
				end agentName from (
				select 
                a.PassportId,
				a.ManageUserPassportId,
				a.OrderID,b.mylevel,e.UserName,e.HashAddress,
				c.GameName,
                c.Id,
                a.BetCoin,
                a.SettlementState,
                a.GameDetailsId,
                a.BetOdds,
				a.BetResult,a.OpenResult,
				case a.OpenResult
				when '无效投注' then 0
				else
					case c.GameName
					when '庄闲' then SUBSTRING(CONVERT(nvarchar(10), betcoin), 0,charindex('.',betcoin)+2)
					when '百家乐' then SUBSTRING(CONVERT(nvarchar(10), betcoin), 0,charindex('.',betcoin)+2)
					else CONVERT(decimal(18,6),SUBSTRING(CONVERT(nvarchar(10), betcoin), 0,charindex('.',betcoin))) end
				end VaildBetAmount,
				case a.OpenResult
				when '无效投注' then a.BetCoin
				else
					case c.GameName
					when '庄闲' then CONVERT(decimal(18,6), SUBSTRING(CONVERT(nvarchar(10), betcoin), charindex('.',betcoin)+2,6))
					when '百家乐' then CONVERT(decimal(18,6), SUBSTRING(CONVERT(nvarchar(10), betcoin), charindex('.',betcoin)+2,6))
					else CONVERT(decimal(18,6), SUBSTRING(CONVERT(nvarchar(10), betcoin), charindex('.',betcoin),6)) end
				end UnVaildBetAmount,
				c.GameFee,
				c.InvalidGameFee,
				a.BetWinCoin,a.BetTime,
				d.UpdateTime,d.status,d.block_number,d.block_hash from (
                select * from 
                (
                SELECT username, ParentPassportId, 0 mylevel, PassportId FROM AdminUser AS b WHERE b.PassportId = @PassportId
                UNION ALL
                SELECT * FROM 
                #userinfo
                )  a
                )b
                join  bet a on  a.ManageUserPassportId = b.PassportId
                left join GameDetails c on a.GameDetailsId = c.GameTypeId and a.ManageUserPassportId=c.ManageUserPassportId
                left join RechargeCashOrder d on a.PassportId = d.PassportId and replace(a.OrderID,'BET','')=replace(d.OrderID,'R','') 
                    --and a.product_ref = d.product_ref
				left join UserInfo e on a.PassportId = e.PassportId
				) betlist
                DROP TABLE #userinfo 
            ";

                SugarParameter[] sugars = new SugarParameter[] {
                new SugarParameter("@PassportId",_PassportId)
            };
                list = db.Ado.SqlQuery<T>(sql, sugars);//比db.SqlQueryable兼容性更强，支持复杂SQL存储过程，缺点没有自带的分页操作
            }
            else if (UserType == "1")
            {
                string sql = $@"
                CREATE TABLE #userinfo
                (
	                username nvarchar(20),
	                ParentId int,
	                MyLevel INT,
	                PassportId bigint
                );
                WITH T 
                AS (SELECT 
			                username,
			                ParentId,
							1 AS mylevel,
			                PassportId
	                FROM 
	                (SELECT username,ParentId,PassportId FROM UserInfo AS b where b.ParentId=@PassportId) AS c
	                UNION ALL
	                SELECT 
			                a.username,
			                a.ParentId,
			                b.mylevel + 1,
			                a.PassportId
	                FROM UserInfo a
		                JOIN T b
			                ON a.ParentId = b.PassportId and a.ParentId=b.ParentId 
			                )
			                INSERT INTO #userinfo
			                (
				                username,
								ParentId,
				                MyLevel,
				                PassportId
			                ) SELECT * FROM T   ORDER BY T.mylevel DESC  OPTION (MAXRECURSION 32700);
                select *,(VaildBetAmount*GameFee/100) gamefeeAmount,(UnVaildBetAmount*InvalidGameFee/100) unvaildbetfeeAmount,
                (select top 1 product_ref from RechargeCashOrder m where replace(m.OrderID,'C','')=replace(betlist.OrderID,'BET','')) as cash_block_hash,
				case mylevel
                when 0 then 'Company'
				when 1 then 'Company'
				else (select top 1 UserName from UserInfo where ParentId=betlist.PassportId)
				end agentName from (
				select 
                a.PassportId,
				a.ManageUserPassportId,
				a.OrderID,b.mylevel,e.UserName,e.HashAddress,
				c.GameName,
                c.Id,
                a.BetCoin,
                a.SettlementState,
                a.GameDetailsId,
                a.BetOdds,
				a.BetResult,a.OpenResult,
				case a.OpenResult
				when '无效投注' then 0
				else
					case c.GameName
					when '庄闲' then SUBSTRING(CONVERT(nvarchar(10), betcoin), 0,charindex('.',betcoin)+2)
					when '百家乐' then SUBSTRING(CONVERT(nvarchar(10), betcoin), 0,charindex('.',betcoin)+2)
					else CONVERT(decimal(18,6),SUBSTRING(CONVERT(nvarchar(10), betcoin), 0,charindex('.',betcoin))) end
				end VaildBetAmount,
				case a.OpenResult
				when '无效投注' then a.BetCoin
				else
					case c.GameName
					when '庄闲' then CONVERT(decimal(18,6), SUBSTRING(CONVERT(nvarchar(10), betcoin), charindex('.',betcoin)+2,6))
					when '百家乐' then CONVERT(decimal(18,6), SUBSTRING(CONVERT(nvarchar(10), betcoin), charindex('.',betcoin)+2,6))
					else CONVERT(decimal(18,6), SUBSTRING(CONVERT(nvarchar(10), betcoin), charindex('.',betcoin),6)) end
				end UnVaildBetAmount,
				c.GameFee,
				c.InvalidGameFee,
				a.BetWinCoin,a.BetTime,
				d.UpdateTime,d.status,d.block_number,d.block_hash from (
                select * from 
                (
                                SELECT username, ParentId, 0 mylevel, PassportId FROM UserInfo AS b WHERE b.PassportId = @PassportId
                                UNION ALL
                                SELECT * FROM 
							    #userinfo
                )  a
                ) b
                join bet a on  a.PassportId = b.PassportId
                left join GameDetails c on a.GameDetailsId = c.GameTypeId and a.ManageUserPassportId=c.ManageUserPassportId
                left join RechargeCashOrder d on a.PassportId = d.PassportId and replace(a.OrderID,'BET','')=replace(d.OrderID,'R','') 
                    --and a.product_ref = d.product_ref
				left join UserInfo e on a.PassportId = e.PassportId
				) betlist
                DROP TABLE #userinfo 
            ";

                SugarParameter[] sugars = new SugarParameter[] {
                new SugarParameter("@PassportId",_PassportId)
            };
                list = db.Ado.SqlQuery<T>(sql, sugars);//比db.SqlQueryable兼容性更强，支持复杂SQL存储过程，缺点没有自带的分页操作
            }

            return list;
        }

        /// <summary>
        /// 获取所有子订单（包含自己）
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="_PassportId"></param>
        /// <param name="sqlwhere">where</param>
        /// <param name="UserType"></param>
        /// <returns></returns>
        private List<T> GetMyChildsRCOrder<T>(Int64 _PassportId, string sqlwhere, string UserType)
        {
            List<T> list = new List<T>();

            if (UserType == "0")
            {
                string sql = $@"
                CREATE TABLE #userinfo
                (
	                username nvarchar(20),
	                ParentPassportId int,
	                MyLevel INT,
	                PassportId bigint
                );
                WITH T 
                AS (SELECT 
			                username,
			                ParentPassportId,
							1 AS mylevel,
			                PassportId
	                FROM 
	                (SELECT username,ParentPassportId,PassportId FROM AdminUser AS b where b.ParentPassportId=@PassportId) AS c
	                UNION ALL
	                SELECT 
			                a.username,
			                a.ParentPassportId,
			                b.mylevel + 1,
			                a.PassportId
	                FROM AdminUser a
		                JOIN T b
			                ON a.ParentPassportId = b.PassportId 
			                )
			                INSERT INTO #userinfo
			                (
				                username,
								ParentPassportId,
				                MyLevel,
				                PassportId
			                ) SELECT * FROM T   ORDER BY T.mylevel DESC  OPTION (MAXRECURSION 32700);
                select b.* from 
                (
                SELECT username, ParentPassportId, 0 mylevel, PassportId FROM AdminUser AS b WHERE b.PassportId = @PassportId
                UNION ALL
                SELECT * FROM 
                #userinfo
                )a
                join RechargeCashOrder b on a.PassportId = b.ManageUserPassportId where 1 = 1 {sqlwhere}
                DROP TABLE #userinfo
            ";

                SugarParameter[] sugars = new SugarParameter[] {
                new SugarParameter("@PassportId",_PassportId)
            };
                list = db.Ado.SqlQuery<T>(sql, sugars);//比db.SqlQueryable兼容性更强，支持复杂SQL存储过程，缺点没有自带的分页操作
            }
            else if (UserType == "1")
            {
                string sql = $@"
                 CREATE TABLE #userinfo
                (
	                username nvarchar(20),
	                ParentId int,
	                MyLevel INT,
	                PassportId bigint
                );
                WITH T 
                AS (SELECT 
			                username,
			                ParentId,
							1 AS mylevel,
			                PassportId
	                FROM 
	                (SELECT username,ParentId,PassportId FROM UserInfo AS b where b.ParentId=@PassportId) AS c
	                UNION ALL
	                SELECT 
			                a.username,
			                a.ParentId,
			                b.mylevel + 1,
			                a.PassportId
	                FROM UserInfo a
		                JOIN T b
			                ON a.ParentId = b.PassportId and a.ParentId=b.ParentId 
			                )
			                INSERT INTO #userinfo
			                (
				                username,
								ParentId,
				                MyLevel,
				                PassportId
			                ) SELECT * FROM T   ORDER BY T.mylevel DESC  OPTION (MAXRECURSION 32700);
                select b.* from 
                (
                                SELECT username, ParentId, 0 mylevel, PassportId FROM UserInfo AS b WHERE b.PassportId = @PassportId
                                UNION ALL
                                SELECT * FROM 
							    #userinfo
                )  a
                join RechargeCashOrder b on a.PassportId = b.PassportId where 1 = 1 {sqlwhere}
                DROP TABLE #userinfo 
            ";

                SugarParameter[] sugars = new SugarParameter[] {
                new SugarParameter("@PassportId",_PassportId)
            };
                list = db.Ado.SqlQuery<T>(sql, sugars);//比db.SqlQueryable兼容性更强，支持复杂SQL存储过程，缺点没有自带的分页操作
            }

            return list;
        }

        #region 返佣
        /// <summary>
        /// 获取我所有的上级返佣(UserType:0为管理员用户，1为会员用户)
        /// </summary>
        /// <param name="UserType">0为管理员用户，1为会员用户</param>
        /// <param name="BetId">投注ID</param>
        /// <param name="BetAmount">投注金额</param>
        /// <param name="CoinType">币种类型</param>
        /// <param name="FromHashAddress">付款哈希地址</param>
        /// <param name="BetPassportId">PassportId(投注人)</param>
        /// <param name="ManageUserPassportId">管理员PassportId</param>
        /// <returns></returns>
        [HttpGet]
        private void GetMyParentRebate(string UserType, int BetId, decimal BetAmount, int CoinType, string FromHashAddress, Int64 BetPassportId, Int64 ManageUserPassportId)
        {
            DateTime t = DateTime.Now;

            var AgentTypeID = db.Queryable<UserInfo>().First(c => c.PassportId == BetPassportId)?.AgentTypeID;
            List<RetuAgentRank> agentRanks = db.Queryable<AgentRank>().LeftJoin<AgentDetails>((c, d) => c.AgentDetailsId == d.Id).
                Select((c, d) => new RetuAgentRank
                {
                    Sort = c.Sort,
                    IsValid = c.IsValid,
                    AddTime = c.AddTime,
                    UpdateTime = c.UpdateTime,
                    AgentDetailsId = c.AgentDetailsId,
                    AgentLevel = c.AgentLevel,
                    AgentValidBet = c.AgentValidBet,
                    RebateRatio1 = c.RebateRatio1,
                    RebateRatio2 = c.RebateRatio2,
                    MaxRebateAmount = c.MaxRebateAmount,
                    RebateGameId = c.RebateGameId,
                    OtherBet = c.OtherBet,
                    OtherRebateRatio = c.OtherRebateRatio,
                    OtherRebateNumber = c.OtherRebateNumber,
                    PassportId = c.PassportId,

                    AgentTypeId = d.AgentId
                }).ToList();
            AgentDetails agent = db.Queryable<AgentDetails>().First(c => c.PassportId == ManageUserPassportId && c.IsValid==1);

            string sql = @"
                CREATE TABLE #userinfo
                (
                    ParentId int,
                    Id int,
                    MyLevel INT,
                    PassportId bigint,
                    HashAddress nvarchar(200),
                    ManageUserPassportId bigint
                );
                WITH T 
                AS (SELECT 
                    ParentId,
                    Id,0 AS mylevel,
                    PassportId,HashAddress,
					ManageUserPassportId
                    FROM 
                    (SELECT ParentId,Id,PassportId,HashAddress,ManageUserPassportId FROM UserInfo AS b where b.PassportId=@PassportId) AS c
                    UNION ALL
                    SELECT 
                    a.ParentId,a.Id,
                    b.mylevel + 1,
                    a.PassportId,a.HashAddress,a.ManageUserPassportId
                    FROM UserInfo a
                    JOIN T b
                    ON a.PassportId = b.ParentId 
                    )
                    INSERT INTO #userinfo
                    (
                    ParentId,
                    Id,
                    MyLevel,
                    PassportId,
                    HashAddress,
                    ManageUserPassportId
                    ) SELECT * FROM T   ORDER BY T.mylevel DESC  OPTION (MAXRECURSION 32700);
                    select * from #userinfo 
                DROP TABLE #userinfo 
            ";

            SugarParameter[] sugars = new SugarParameter[] {
                new SugarParameter("@PassportId",BetPassportId)
            };
            List<MyParent> list = db.Ado.SqlQuery<MyParent>(sql, sugars);//比db.SqlQueryable兼容性更强，支持复杂SQL存储过程，缺点没有自带的分页操作
            LogHelper.Debug(JsonConvert.SerializeObject(list));

            List<RebateDetails> rebates = new List<RebateDetails>();
            var RebateAmount = 0m;
            foreach (var item in list)
            {
                
                long _PassportId = item.PassportId;//PassportId(返佣人)
                string ToHashAddress = item.HashAddress;//PassportId(返佣地址)
                RetuAgentRank retuAgentRanks = (from c in agentRanks
                                                where c.AgentTypeId == AgentTypeID && c.PassportId == _PassportId &&
                                                    c.AgentLevel == item.MyLevel
                                                select c).FirstOrDefault();
                //社区收益
                {
                    //返佣比例
                    var RebateRatio1 = retuAgentRanks?.RebateRatio1 ?? 1;
                    //返佣极差
                    var RebateRatio2 = retuAgentRanks?.RebateRatio2 ?? 50;
                    //返佣上限
                    var MaxRebateAmount = retuAgentRanks?.MaxRebateAmount ?? 100;
                    //自投返佣(0关闭，1开启)
                    var AgentBetIsRebate = agent.AgentBetIsRebate;
                    //自投返佣比例
                    var AgentBetRebateRatio = agent.AgentBetRebateRatio;

                    //返佣金额
                    RebateAmount = BetAmount * (RebateRatio1 / 100);
                    if (AgentBetIsRebate==1)
                    {
                        RebateAmount = BetAmount * (AgentBetRebateRatio / 100);
                    }
                    if (AgentBetIsRebate==0)
                    {
                        RebateAmount = BetAmount * (AgentBetRebateRatio / 100);
                    }
                    if (AgentBetIsRebate == 0)
                    {
                        if (BetPassportId == _PassportId)
                        {
                            continue;
                        }
                    }

                    var _RebateAmount = db.Queryable<RebateDetails>().Where(c => c.PassportId == _PassportId).Sum(c => c.RebateAmount);
                    if (_RebateAmount >= MaxRebateAmount)
                    {
                        continue;
                    }
                    RebateDetails bate = new()
                    {
                        AddTime = t,
                        UpdateTime = t,
                        IsValid = 1,
                        Sort = 1,
                        Ip = YRHelper.GetClientIPAddress(HttpContext),

                        PassportId = _PassportId,
                        ToHashAddress = ToHashAddress ?? string.Empty,
                        BetId = BetId,
                        BetAmount = BetAmount,
                        RebateAmount = RebateAmount,
                        CoinType = CoinType,
                        FromHashAddress = FromHashAddress,
                        BetPassportId = BetPassportId,
                        ManageUserPassportId = ManageUserPassportId,
                        CalculationState = 0 //返佣计算状态(0未计算，1已计算)
                    };
                    rebates.Add(bate);
                }
            }
            db.Insertable(rebates).ExecuteCommand();
        }

        /// <summary>
        /// 获取返佣列表(UserType:0为管理员用户，1为会员用户)
        /// </summary>
        /// <param name="OpenId"></param>
        /// <param name="UserType">(UserType:0为管理员用户，1为会员用户)</param>
        /// <param name="FromHashAddress">用户地址</param>
        /// <param name="AddTime1">返佣时间</param>
        /// <param name="AddTime2">返佣时间</param>
        /// <param name="PageIndex"></param>
        /// <param name="PageSize"></param>
        /// <returns></returns>
        [HttpGet]
        public MsgInfo<List<RebateDetails>> GetRebate(string OpenId, string UserType, string FromHashAddress, string AddTime1, string AddTime2, int PageIndex, int PageSize = 20)
        {
            MsgInfo<List<RebateDetails>> info = new();
            long _PassportId = IsLogin(OpenId, UserType);
            if (_PassportId <= 0)
            {
                info.code = 400;
                info.msg = "Please login";
                return info;
            }

            DateTime t1 = DateTime.Now;
            DateTime.TryParse(AddTime1, out t1);
            DateTime t2 = DateTime.Now;
            DateTime.TryParse(AddTime2, out t2);
            //List<RebateDetails> list = GetMyChildsAllRebate(_PassportId, UserType);
            List<RebateDetails> list = db.Queryable<RebateDetails>().Where(c => c.PassportId == _PassportId).ToList();
            int RoleId = GetRoleId(_PassportId, UserType);
            if (RoleId == 1)
            {
                list = db.Queryable<RebateDetails>().ToList();
            }

            if (!string.IsNullOrWhiteSpace(FromHashAddress))
            {
                list = (from c in list where c.FromHashAddress == FromHashAddress select c).ToList();
            }
            //时间
            if (!string.IsNullOrWhiteSpace(AddTime1) && !string.IsNullOrWhiteSpace(AddTime2))
            {
                list = (from c in list where c.AddTime >= t1 && c.AddTime <= t2 select c).ToList();
            }
            else if (!string.IsNullOrWhiteSpace(AddTime1))
            {
                list = (from c in list where c.AddTime >= t1 select c).ToList();
            }
            else if (!string.IsNullOrWhiteSpace(AddTime2))
            {
                list = (from c in list where c.AddTime <= t2 select c).ToList();
            }
            info.data_count = list.Count;
            if (PageIndex > 0)
            {
                list = (from c in list orderby c.AddTime descending select c).Skip((PageIndex - 1) * PageSize).Take(PageSize).ToList();
            }

            info.code = 200;
            info.msg = "success";
            info.data = list;
            return info;
        }

        #region 任务调度
        /// <summary>
        /// 返佣出金
        /// </summary>
        /// <param name="OpenId"></param>
        /// <returns></returns>
        [HttpPost]
        private MsgInfo<List<long>> PostRebate(string OpenId)
        {
            MsgInfo<List<long>> info = new();
            DateTime t = DateTime.Now;
            //long _PassportId = IsLogin(OpenId);
            //if (_PassportId <= 0)
            //{
            //    info.code = 400;
            //    info.msg = "Please login";
            //    return info;
            //}
            long _PassportId = 502423741;

            decimal dec_MinCashAmount = 100;
            var MinCashAmount = db.Queryable<Sys>().First(c => c.Keys == "MinCashAmount")?.Value ?? "100";
            decimal.TryParse(MinCashAmount, out dec_MinCashAmount);
            var userinfo_passportId = db.Queryable<UserInfo>().Select(c => c.PassportId).ToList();
            var admin_passportId = db.Queryable<AdminUser>().Select(c => c.PassportId).ToList();
            List<long> list = new List<long>();
            list.AddRange(userinfo_passportId);
            list.AddRange(admin_passportId);

            List<Task> tasks = new List<Task>();
            string sql = @"
                  select PassportId,ToHashAddress,SUM(RebateAmount) RebateAmount,MAX(Id) Id  from RebateDetails where CalculationState=0 group by PassportId,ToHashAddress 
                        having SUM(RebateAmount)>@RebateAmount
            ";
            SugarParameter[] sugars = new SugarParameter[] {
                new SugarParameter("@RebateAmount",dec_MinCashAmount)
            };
            List<RebateDetails> rebateDetails = db.Ado.SqlQuery<RebateDetails>(sql, sugars);//比db.SqlQueryable兼容性更强，支持复杂SQL存储过程，缺点没有自带的分页操作
            if (rebateDetails.Count < 50)
            {
                tasks.Add(Task.Run(() =>
                {
                    GetRebateCalculation(0, rebateDetails.Count, rebateDetails);
                }));
            }
            else
            {
                int number = rebateDetails.Count / 50;
                int yushu = rebateDetails.Count % 50;
                for (int i = 0; i < rebateDetails.Count; i++)
                {
                    int index = i;
                    int SPage = (number * index);
                    int EPage = (number * (index + 1));
                    tasks.Add(
                        Task.Run(() =>
                        {
                            GetRebateCalculation(SPage, EPage, rebateDetails);
                        })
                        );
                }
                if (yushu > 0)
                {
                   
                    tasks.Add(
                        Task.Run(() =>
                        {
                            GetRebateCalculation((number * 50), rebateDetails.Count, rebateDetails);
                        })
                        );
                }
            }
            Task.WaitAll(tasks.ToArray());

            info.code = 200;
            info.msg = "success";
            info.data = list;
            return info;
        }

        private void GetRebateCalculation(Int64 SPage, Int64 EPage, List<RebateDetails> list)
        {
            DateTime t = DateTime.Now;
            List<RebateDetails> rebateDetails = new List<RebateDetails>();
            for (int i = 0; i < list.Count; i++)
            {
                RebateDetails details = new RebateDetails();
                details.AddTime = list[i].AddTime;
                details.UpdateTime = t;
                details.IsValid = list[i].IsValid;
                details.Sort = list[i].Sort;
                details.Ip = list[i].Ip;

                details.PassportId = list[i].PassportId;
                details.ToHashAddress = list[i].ToHashAddress;
                details.BetId = list[i].BetId;
                details.BetAmount = list[i].BetAmount;
                details.RebateAmount = list[i].RebateAmount;
                details.FromHashAddress = list[i].FromHashAddress;
                details.BetPassportId = list[i].BetPassportId;
                details.ManageUserPassportId = list[i].ManageUserPassportId;
                details.CalculationState = 1;
                details.SettlementState = 1;

                list.Add(details);

                #region 发起出金
                Task task = Task.Run(() =>
                {
                    //出金
                    {
                        var BateCashOrderId = "Bate" + t.Year.ToString() + t.Month.ToString() + t.Day.ToString() + t.Hour.ToString() + t.Minute.ToString() + t.Second.ToString() + t.Millisecond.ToString();
                        //调用出金接口
                        PostCash(BateCashOrderId, list[i].RebateAmount, list[i].ToHashAddress, list[i].ManageUserPassportId, "", "","","",3);
                    }
                });
                #endregion
            }
            db.Fastest<RebateDetails>().BulkCopy(list);
        }
        #endregion

        /// <summary>
        /// 获取可领返佣金额
        /// </summary>
        /// <param name="OpenId"></param>
        /// <param name="UserType"></param>
        /// <param name="Cash">0查看，1领取</param>
        /// <returns></returns>
        [HttpGet]
        public MsgInfo<List<Dictionary<string, decimal>>> GetCashRebate(string OpenId, string UserType, int Cash = 0)
        {
            DateTime t = DateTime.Now;
            MsgInfo<List<Dictionary<string, decimal>>> info = new();
            List<Dictionary<string, decimal>> list = new();
            long _PassportId = IsLogin(OpenId, UserType);
            if (_PassportId <= 0)
            {
                info.code = 400;
                info.msg = "Please login";
                return info;
            }

            List<RebateDetails> rebates = db.Queryable<RebateDetails>().Where(c => c.CalculationState == 0 && c.PassportId == _PassportId).ToList();
            if (rebates == null)
            {
                info.code = 200;
                info.msg = "success";
                return info;
            }
            //decimal RebateAmount = rebates.Sum(c => c.RebateAmount);
            var RebateGroupByCoinType = (from c in rebates group c by c.CoinType into g select new { cointype = g.Key, sumamount = g.Sum(c => c.RebateAmount) }).ToList();
            foreach (var item in RebateGroupByCoinType)
            {
                Dictionary<string, decimal> keys = new Dictionary<string, decimal>();
                keys.Add(item.cointype.ToString(), item.sumamount);
                list.Add(keys);
            }


            if (Cash == 0)
            {
                info.code = 200;
                info.msg = "success";
                info.data = list;
                return info;
            }
            //if (RebateAmount == 0)
            //{
            //    info.code = 200;
            //    info.msg = "success";
            //    info.data = RebateAmount;
            //    return info;
            //}

            SugarParameter[] sugars = new SugarParameter[] {
                    new SugarParameter("@PassportId",_PassportId)
            };
            int m = db.Ado.ExecuteCommand("update RebateDetails set CalculationState = 1 where CalculationState = 0 and PassportId = @PassportId", sugars);
            if (m < rebates.Count)
            {
                LogHelper.Debug($"方法：GetCashRebate,执行成功{m}条");
            }

            foreach (var item in list)
            {
                var RebateAmount = item.Select(c => c.Value).FirstOrDefault();
                var BateCashOrderId = "BATE" + t.Year.ToString() + t.Month.ToString() + t.Day.ToString() + t.Hour.ToString() + t.Minute.ToString() + t.Second.ToString() + t.Millisecond.ToString();
                Rebate rebate = new()
                {
                    AddTime = t,
                    UpdateTime = t,
                    IsValid = 1,
                    Sort = 1,
                    Ip = YRHelper.GetClientIPAddress(HttpContext),

                    PassportId = _PassportId,
                    OrderID = BateCashOrderId,
                    RebateAmount = RebateAmount,
                    SettlementState = 0
                };
                int n = db.Insertable<Rebate>(rebate).ExecuteCommand();
                if (n < 1)
                {
                    info.code = 400;
                    info.msg = "fail";
                    return info;
                }


                string ToHashAddress = db.Queryable<UserInfo>().First(c => c.PassportId == _PassportId)?.HashAddress;
                if (!string.IsNullOrWhiteSpace(ToHashAddress))
                {
                    PostCash(BateCashOrderId, RebateAmount, ToHashAddress, 0, "", "");
                }
                else
                {
                    LogHelper.Debug($"方法：GetCashRebate，{_PassportId}没有返佣收款地址");
                }
            }

            info.code = 200;
            info.msg = "success";
            return info;
        }

        /// <summary>
        /// 获取领取返佣列表
        /// </summary>
        /// <param name="OpenId"></param>
        /// <param name="UserType"></param>
        /// <returns></returns>
        [HttpGet]
        public MsgInfo<List<Rebate>> GetCashRebateList(string OpenId, string UserType)
        {
            MsgInfo<List<Rebate>> info = new();
            long _PassportId = IsLogin(OpenId, UserType);
            if (_PassportId <= 0)
            {
                info.code = 400;
                info.msg = "Please login";
                return info;
            }

            List<Rebate> rebates = db.Queryable<Rebate>().Where(c => c.PassportId == _PassportId).ToList();

            info.code = 200;
            info.msg = "success";
            info.data = rebates;
            return info;
        }

        /// <summary>
        /// 节点返佣
        /// </summary>
        /// <param name="OpenId"></param>
        /// <returns></returns>
        [HttpGet]
        public MsgInfo<string> GetNodeRebate(string OpenId)
        {
            MsgInfo<string> info = new();
            long _PassportId = IsLogin(OpenId);
            if (_PassportId <= 0)
            {
                info.code = 400;
                info.msg = "Please login";
                return info;
            }

            List<Bet> bets = db.Queryable<Bet>().ToList();
            List<UserInfo> allUsers = db.Queryable<UserInfo>().ToList();
            List<UserInfo> userInfos = (from c in allUsers where c.RoleId == 4 && c.ParentId == 999999999 select c).ToList();
            List<RetuAgentRank> agentRanks = db.Queryable<AgentRank>().LeftJoin<AgentDetails>((c, d) => c.AgentDetailsId == d.Id).
                Select((c, d) => new RetuAgentRank {
                    Sort = c.Sort,
                    IsValid = c.IsValid,
                    AddTime = c.AddTime,
                    UpdateTime = c.UpdateTime,
                    AgentDetailsId = c.AgentDetailsId,
                    AgentLevel = c.AgentLevel,
                    AgentValidBet = c.AgentValidBet,
                    RebateRatio1 = c.RebateRatio1,
                    RebateRatio2 = c.RebateRatio2,
                    MaxRebateAmount = c.MaxRebateAmount,
                    RebateGameId = c.RebateGameId,
                    OtherBet = c.OtherBet,
                    OtherRebateRatio = c.OtherRebateRatio,
                    OtherRebateNumber = c.OtherRebateNumber,
                    PassportId = c.PassportId,

                    AgentTypeId = d.AgentId,
                    ManageUserPassportId = d.PassportId
                }).ToList();
            List<Task> tasks = new List<Task>();
            if (userInfos.Count < 50)
            {
                tasks.Add(Task.Run(() =>
                {
                    GetMyNodeRebate(userInfos, agentRanks, allUsers, bets);
                }));
            }
            else
            {
                int number = userInfos.Count / 50;
                int yushu = userInfos.Count % 50;
                for (int i = 0; i < 50; i++)
                {
                    List<UserInfo> currentUserInfos = new List<UserInfo>();
                    int index = i;
                    int SPage = (number * index);
                    int EPage = (number * (index + 1));
                    for (int j = EPage; j < EPage; j++)
                    {
                        currentUserInfos.Add(userInfos[j]);
                    }
                    tasks.Add(
                        Task.Run(() =>
                        {
                            GetMyNodeRebate(currentUserInfos, agentRanks, allUsers, bets);
                        })
                        );
                }
                List<UserInfo> users_yushu = new List<UserInfo>();
                List<UserInfo> current_yushu = new List<UserInfo>();
                for (int j = number * 50; j < userInfos.Count; j++)
                {
                    current_yushu.Add(userInfos[j]);
                }

                if (yushu > 0)
                {
                    tasks.Add(
                        Task.Run(() =>
                        {
                            GetMyNodeRebate(current_yushu, agentRanks, allUsers, bets);
                        })
                        );
                }
            }
            Task.WaitAll(tasks.ToArray());


            info.code = 200;
            info.msg = "success";
            return info;
        }

        /// <summary>
        /// 插入节点返佣
        /// </summary>
        /// <param name="list"></param>
        /// <param name="agentRanks">代理层级</param>
        /// <param name="allUsers">代理与会员</param>
        /// <param name="bets">投注</param>
        private void GetMyNodeRebate(List<UserInfo> list, List<RetuAgentRank> agentRanks, List<UserInfo> allUsers, List<Bet> bets)
        {
            //1.查找每个会员的下级会员的投注量
            //2.下级会员投注量 (xx) 达到要求的会员数量超过 xx 个
            //3.满足条件，节点返佣
            for (int i = 0; i < list.Count; i++)
            {
                var current_passportid = list[i].PassportId;//顶级代理passportid
                var current_agenttypeid = list[i].AgentTypeID;//代理类型ID
                var current_manageuserpassportid = list[i].ManageUserPassportId;//代理所在商户
                LogHelper.Debug(current_passportid.ToString());
                List<MyChild> childrens = GetMyChildByLevel(current_passportid);
                LogHelper.Debug(JsonConvert.SerializeObject(childrens));

                #region 自己
                RetuAgentRank retuAgentRanksTop = (from c in agentRanks
                                                   where c.AgentTypeId == current_agenttypeid &&
                                                       c.AgentLevel == 1 && c.ManageUserPassportId==current_manageuserpassportid
                                                   select c).FirstOrDefault();
                if (retuAgentRanksTop==null)
                {
                    continue;
                }
                //额外流水
                var OtherBetTop = retuAgentRanksTop.OtherBet;
                //额外反水
                var OtherRebateRatioTop = retuAgentRanksTop.OtherRebateRatio;
                //额外人数
                var OtherRebateNumberTop = retuAgentRanksTop.OtherRebateNumber;
                List<UserInfo> users = db.Queryable<UserInfo>().Where(c => c.ParentId == current_passportid).ToList();
                if (users.Count < OtherRebateNumberTop)
                {
                    continue;
                }
                foreach (var item in users)
                {
                    var c_passportid = item.PassportId;
                    //节点收益
                    {
                        List<Bet> bets1 = (from c in bets where c.PassportId == c_passportid select c).ToList();
                        var bet_type_amount_list = bets1.GroupBy(c => c.CoinType).Select(g => new { CoinType = g.Key, SumBetCoin = g.Sum(c => c.BetCoin) }).ToList();
                        //币种分类
                        foreach (var bettype in bet_type_amount_list)
                        {
                            var bet_coin_type = bettype.CoinType;
                            var bet_sum_amount = bettype.SumBetCoin;

                            DateTime t = DateTime.Now;
                            var rebateAmount = bet_sum_amount * OtherRebateRatioTop / 100;
                            var OrderID = "N" + t.Year.ToString() + t.Month.ToString() + t.Day.ToString() + t.Hour.ToString() + t.Minute.ToString() + t.Second.ToString() + t.Millisecond.ToString(); ;
                            if (rebateAmount > 0)
                            {
                                Rebate bate = new()
                                {
                                    AddTime = t,
                                    UpdateTime = t,
                                    IsValid = 1,
                                    Sort = 1,
                                    Ip = YRHelper.GetClientIPAddress(HttpContext),

                                    PassportId = current_passportid,
                                    OrderID = OrderID,
                                    CoinType = bet_coin_type,
                                    RebateAmount = rebateAmount,//返佣金额
                                    SettlementState = 0//返佣结算状态(0未结算，1已结算)
                                };

                                int n = db.Insertable(bate).ExecuteCommand();
                                if (n < 1)
                                {
                                    LogHelper.Debug($"ActionName(GetMyNodeRebate),{JsonConvert.SerializeObject(bate)}");
                                }
                            }
                        }

                    }
                }
                #endregion

                #region 下级
                foreach (var item in childrens)
                {
                    var c_passportid = item.PassportId;
                    var c_mylevel = item.MyLevel;
                    RetuAgentRank retuAgentRanks = (from c in agentRanks
                                                    where c.AgentTypeId == item.AgentTypeId && c.PassportId == item.ManageUserPassportId &&
                                                        c.AgentLevel == item.MyLevel
                                                    select c).FirstOrDefault();
                    if (retuAgentRanks == null)
                    {
                        continue;
                    }
                    //节点收益
                    {
                        //额外流水
                        var OtherBet = retuAgentRanks.OtherBet;
                        //额外反水
                        var OtherRebateRatio = retuAgentRanks.OtherRebateRatio;
                        //额外人数
                        var OtherRebateNumber = retuAgentRanks.OtherRebateNumber;

                        List<long> userInfos = (from c in allUsers where c.ParentId == c_passportid select c.PassportId).ToList();
                        if (userInfos.Count < OtherRebateNumber)
                        {
                            continue;
                        }

                        var temp = 0;
                        var sum_amount = 0m;
                        for (int m = 0; m < userInfos.Count; m++)
                        {
                            var current_amount = (from c in bets where c.PassportId == userInfos[m] select c).Sum(c => c.BetCoin);
                            if (current_amount > OtherBet)
                            {
                                sum_amount += current_amount;
                                temp++;
                            }
                        }
                        if (temp < OtherRebateNumber)
                        {
                            continue;
                        }

                        DateTime t = DateTime.Now;
                        var rebateAmount = sum_amount * OtherRebateRatio / 100;
                        var OrderID = "N" + t.Year.ToString() + t.Month.ToString() + t.Day.ToString() + t.Hour.ToString() + t.Minute.ToString() + t.Second.ToString() + t.Millisecond.ToString(); ;
                        if (rebateAmount > 0)
                        {
                            Rebate bate = new()
                            {
                                AddTime = t,
                                UpdateTime = t,
                                IsValid = 1,
                                Sort = 1,
                                Ip = YRHelper.GetClientIPAddress(HttpContext),

                                PassportId = current_passportid,
                                OrderID = OrderID,
                                CoinType = 1,
                                RebateAmount = rebateAmount,//返佣金额
                                SettlementState = 0//返佣结算状态(0未结算，1已结算)
                            };

                            int n = db.Insertable(bate).ExecuteCommand();
                            if (n < 1)
                            {
                                LogHelper.Debug($"ActionName(GetMyNodeRebate),{JsonConvert.SerializeObject(bate)}");
                            }
                        }
                    }
                }
                #endregion

            }
        }
        #endregion


        #region 权限控制
        /// <summary>
        /// 获取角色
        /// </summary>
        /// <param name="OpenId">OpenId</param>
        /// <param name="RoleName">角色名称</param>
        /// <param name="PageIndex">当前页</param>
        /// <param name="PageSize">每页数量</param>
        /// <returns></returns>
        [HttpGet]
        public MsgInfo<List<AdminRole>> GetRole(string OpenId, string RoleName, int PageIndex, int PageSize = 20)
        {
            DateTime t = DateTime.Now;
            MsgInfo<List<AdminRole>> info = new();
            long _PassportId = IsLogin(OpenId);
            if (_PassportId <= 0)
            {
                info.code = 400;
                info.msg = "Please login";
                return info;
            }

            List<AdminRole> list = db.Queryable<AdminRole>().Where(c => c.ManageUserPassportId == _PassportId).ToList();
            if (!string.IsNullOrWhiteSpace(RoleName))
            {
                list = (from c in list where c.RoleName == RoleName select c).ToList();
            }
            info.data_count = list.Count;
            if (PageIndex > 0)
            {
                list = (from c in list orderby c.AddTime descending select c).Skip((PageIndex - 1) * PageSize).Take(PageSize).ToList();
            }

            info.code = 200;
            info.msg = "success";
            info.data = list;
            return info;
        }

        /// <summary>
        /// 添加/修改角色(添加RoleId为空)
        /// </summary>
        /// <param name="param">{"OpenId":"9e26f6e2244f7e34ed8b4eabdcb2f9c4","RoleName":"管理员","RoleDetails":"管理员","RoleId":""}</param>
        /// <returns></returns>
        [HttpPost]
        public MsgInfo<string> PostRole(dynamic param)
        {
            MsgInfo<string> info = new();
            DateTime t = DateTime.Now;
            var data = JsonConvert.DeserializeObject<dynamic>(param.ToString());
            string OpenId = data.OpenId.ToString();
            if (Object.ReferenceEquals(null, data))
            {
                info.code = 400;
                info.msg = "Network Exception";
                return info;
            }
            long _PassportId = IsLogin(OpenId);
            if (_PassportId <= 0)
            {
                info.code = 400;
                info.msg = "Please login";
                return info;
            }

            string RoleName = data.RoleName.ToString();
            string RoleDetails = data.RoleDetails.ToString();
            string RoleId = data.RoleId.ToString();
            if (string.IsNullOrEmpty(RoleId))
            {
                AdminRole role = new()
                {
                    AddTime = t,
                    UpdateTime = t,
                    IsValid = 1,
                    Sort = 1,
                    Ip = YRHelper.GetClientIPAddress(HttpContext),

                    RoleName = RoleName,
                    RoleDetails = RoleDetails,
                    ManageUserPassportId = _PassportId
                };
                int m = db.Insertable(role).ExecuteCommand();
                if (m < 1)
                {
                    info.code = 400;
                    info.msg = "Network Exception";
                    return info;
                }
            }
            else
            {
                int _RoleId = 0;
                int.TryParse(RoleId, out _RoleId);
                int m = db.Updateable<AdminRole>().SetColumns(c => c.UpdateTime == t).
                    SetColumns(c => c.RoleName == RoleName).
                    SetColumns(c => c.RoleDetails == RoleDetails).
                    Where(c => c.Id == _RoleId).ExecuteCommand();
                if (m < 1)
                {
                    info.code = 400;
                    info.msg = "Network Exception";
                    return info;
                }
            }


            info.code = 200;
            info.msg = "success";

            return info;
        }

        /// <summary>
        /// 获取页面
        /// </summary>
        /// <param name="OpenId">OpenId</param>
        /// <param name="UserType">(UserType:0为管理员用户，1为会员用户)</param>
        /// <param name="PageIndex">当前页</param>
        /// <param name="PageSize">每页数量</param>
        /// <returns></returns>
        [HttpGet]
        public MsgInfo<List<RetuAdminPage>> GetPage(string OpenId, string UserType, int PageIndex, int PageSize = 20)
        {
            MsgInfo<List<RetuAdminPage>> info = new();
            long _PassportId = IsLogin(OpenId);
            if (_PassportId <= 0)
            {
                info.code = 400;
                info.msg = "Please login";
                return info;
            }
            List<RetuAdminPage> allPages = db.Queryable<RetuAdminPage>().ToList();
            List<RetuAdminPage> pages = db.Queryable<RetuAdminPage>()
            .Where(o => o.ManageUserPassportId == _PassportId && o.ParentId == 0)
            .Select(o => new RetuAdminPage
            {
                Id = o.Id,
                PageName = o.PageName,
                PageUrl = o.PageUrl,
                PageIcon = o.PageIcon,
                icon = o.PageIcon,
                ParentId = o.ParentId
            })
            .ToList();

            pages.ForEach(x => x.Childrens = allPages.Where(c => c.ParentId == x.Id).Select(o => new RetuAdminPage()
            {
                Id = o.Id,
                PageName = o.PageName,
                PageUrl = o.PageUrl,
                PageIcon = o.PageIcon,
                icon = o.PageIcon,
                ParentId = o.ParentId
            }).ToList());

            info.code = 200;
            info.msg = "success";
            info.data = pages;
            return info;

        }

        /// <summary>
        /// 添加/修改页面(添加PageId为空,ParentId=0为根目录)
        /// </summary>
        /// <param name="param">{"OpenId":"9e26f6e2244f7e34ed8b4eabdcb2f9c4","PageName":"订单列表","PageUrl":"","ParentId":"0","PageId":""}</param>
        /// <returns></returns>
        [HttpPost]
        public MsgInfo<string> PostPage(dynamic param)
        {
            MsgInfo<string> info = new();
            DateTime t = DateTime.Now;
            var data = JsonConvert.DeserializeObject<dynamic>(param.ToString());
            string OpenId = data.OpenId.ToString();
            if (Object.ReferenceEquals(null, data))
            {
                info.code = 400;
                info.msg = "Network Exception";
                return info;
            }
            long _PassportId = IsLogin(OpenId);
            if (_PassportId <= 0)
            {
                info.code = 400;
                info.msg = "Please login";
                return info;
            }
            int _RoleId = GetRoleId(_PassportId);
            if (_RoleId != 1)
            {
                info.code = 400;
                info.msg = "No permission";
                return info;
            }

            string PageName = data.PageName.ToString();
            string PageUrl = data.PageUrl.ToString();
            int ParentId = int.Parse(data.ParentId.ToString());
            string PageId = data.PageId.ToString();

            if (string.IsNullOrEmpty(PageId))
            {
                AdminPage role = new()
                {
                    Id = db.Queryable<AdminPage>().Max(c => c.Id) + 1,
                    AddTime = t,
                    UpdateTime = t,
                    IsValid = 1,
                    Sort = 1,
                    Ip = YRHelper.GetClientIPAddress(HttpContext),

                    PageName = PageName,
                    PageUrl = PageUrl,
                    ParentId = ParentId,
                    ManageUserPassportId = _PassportId
                };
                int m = db.Insertable(role).ExecuteCommand();
                if (m < 1)
                {
                    info.code = 400;
                    info.msg = "Network Exception";
                    return info;
                }
            }
            else
            {
                int _PageId = 0;
                int.TryParse(PageId, out _PageId);
                int m = db.Updateable<AdminPage>().SetColumns(c => c.UpdateTime == t).
                    SetColumns(c => c.PageName == PageName).
                    SetColumns(c => c.PageUrl == PageUrl).
                    SetColumns(c => c.ParentId == ParentId).
                    Where(c => c.Id == _PageId).ExecuteCommand();
                if (m < 1)
                {
                    info.code = 400;
                    info.msg = "Network Exception";
                    return info;
                }
            }
            info.code = 200;
            info.msg = "success";

            return info;
        }

        /// <summary>
        /// 登录用户获取权限(UserType:0为管理员用户，1为会员用户)
        /// </summary>
        /// <param name="OpenId">OpenId</param>
        /// <param name="UserType">(UserType:0为管理员用户，1为会员用户)</param>
        /// <returns></returns>
        [HttpGet]
        public MsgInfo<List<RetuAdminPage>> GetPower(string OpenId, string UserType)
        {
            LogHelper.Debug($"GetPower:{OpenId}|{UserType}");
            MsgInfo<List<RetuAdminPage>> info = new();
            long _PassportId = IsLogin(OpenId, UserType);
            if (_PassportId <= 0)
            {
                info.code = 400;
                info.msg = "Please login";
                return info;
            }

            int _RoleId = GetRoleId(_PassportId, UserType);
            int isgoogle = 0;
            if (UserType == "1")
            {
                long.TryParse(db.Queryable<UserInfo>().First(c => c.PassportId == _PassportId).ManageUserPassportId.ToString(), out _PassportId);
            }
            else
            {
                AdminUser admin = db.Queryable<AdminUser>().First(c => c.PassportId == _PassportId);
                if (admin != null)
                {
                    if (admin.ParentPassportId == 0)
                    {
                        _PassportId = admin.AdminPassportId;
                    }
                    isgoogle = admin.IsGoogle;
                }
            }            

            List<RetuAdminPage> allPages = db.Queryable<AdminPage>()
                            .LeftJoin<AdminPower>((o, i) => o.Id == i.PageId)
                            .Where((o, i) => i.RoleId == _RoleId && i.ManageUserPassportId == _PassportId && o.ManageUserPassportId == _PassportId)
                            .Select((o, i) => new RetuAdminPage
                            {
                                Id = o.Id,
                                PageName = o.PageName,
                                PageUrl = o.PageUrl,
                                PageIcon = o.PageIcon,
                                icon = o.PageIcon,
                                ParentId = o.ParentId,
                                IsGoogle = isgoogle,
                                RoleId = i.RoleId
                            }).ToList();
            List<RetuAdminPage> pages = db.Queryable<AdminPage>()
            .LeftJoin<AdminPower>((o, i) => o.Id == i.PageId)
            .Where((o, i) => i.RoleId == _RoleId && o.ParentId == 0 && i.ManageUserPassportId == _PassportId && o.ManageUserPassportId == _PassportId)
            .Select((o, i) => new RetuAdminPage
            {
                Id = o.Id,
                PageName = o.PageName,
                PageUrl = o.PageUrl,
                PageIcon = o.PageIcon,
                icon = o.PageIcon,

                ParentId = o.ParentId,
                IsGoogle = isgoogle,
                RoleId = i.RoleId
            })
            .ToList();

            pages.ForEach(x => x.Childrens = allPages.Where(c => c.ParentId == x.Id).Select(o => new RetuAdminPage()
            {
                Id = o.Id,
                PageName = o.PageName,
                PageUrl = o.PageUrl,
                PageIcon = o.PageIcon,
                icon = o.PageIcon,
                ParentId = o.ParentId,
                IsGoogle = isgoogle,
                RoleId = o.RoleId,
            }).ToList());

            pages = (from o in pages
                     orderby o.Id
                     select new RetuAdminPage
                     {
                         Id = o.Id,
                         PageName = o.PageName,
                         PageUrl = o.PageUrl,
                         PageIcon = o.PageIcon,
                         icon = o.PageIcon,
                         ParentId = o.ParentId,
                         IsGoogle = isgoogle,
                         RoleId = o.RoleId,
                         Childrens = o.Childrens,
                         RoleType = db.Queryable<AdminRole>().First(c => c.Id == o.RoleId).RoleType
                     }).ToList();

            //pages = (from o in pages
            //         orderby o.Id
            //         select o).ToList();

            info.code = 200;
            info.msg = "success";
            info.data = pages;
            return info;
        }

        /// <summary>
        /// 获取角色权限
        /// </summary>
        /// <param name="OpenId">OpenId</param>
        /// <param name="RoleId">角色ID</param>
        /// <returns></returns>
        [HttpGet]
        public MsgInfo<List<RetuAdminPage>> GetPowerByRole(string OpenId,string RoleId)
        {
            MsgInfo<List<RetuAdminPage>> info = new();
            long _PassportId = IsLogin(OpenId);
            if (_PassportId <= 0)
            {
                info.code = 400;
                info.msg = "Please login";
                return info;
            }
            int _RoleId = 0;
            int.TryParse(RoleId, out _RoleId);

            string sql_select_role = @"
                select m.*,n.RoleId from (
                select distinct a.*,b.PageId from (select a.* from AdminPage a where a.ManageUserPassportId = @PassportId) a left join AdminPower b on  a.Id = b.PageId and a.ManageUserPassportId = @PassportId
                ) m left join AdminPower n on m.PageId = n.PageId and n.RoleId = @RoleId and m.ManageUserPassportId = n.ManageUserPassportId
            ";
            SugarParameter[] sugars = new SugarParameter[] {
                new SugarParameter("@PassportId",_PassportId),
                new SugarParameter("@RoleId",_RoleId)
            };
            List<RetuAdminPage> allPages = db.Ado.SqlQuery<RetuAdminPage>(sql_select_role, sugars);//比db.SqlQueryable兼容性更强，支持复杂SQL存储过程，缺点没有自带的分页操作
            var isgoogle = (int)db.Queryable<AdminUser>().First(d => d.OpenId == OpenId).IsGoogle;
            List<RetuAdminPage> pages = (from o in allPages
                                         where o.ParentId == 0 select  new RetuAdminPage
                            {
                                Id = o.Id,
                                PageName = o.PageName,
                                PageUrl = o.PageUrl,
                                PageIcon = o.PageIcon,
                                icon = o.PageIcon,
                                ParentId = o.ParentId,
                                RoleId = o.RoleId,
                                IsGoogle = isgoogle
                            }).ToList();
            pages.ForEach(x => x.Childrens = allPages.Where(c => c.ParentId == x.Id).Select(o => new RetuAdminPage()
            {
                Id = o.Id,
                PageName = o.PageName,
                PageUrl = o.PageUrl,
                PageIcon = o.PageIcon,
                icon = o.PageIcon,
                ParentId = o.ParentId,
                RoleId = o.RoleId,
                IsGoogle = isgoogle
            }).ToList());

            info.code = 200;
            info.msg = "success";
            info.data = pages;
            return info;
        }

        /// <summary>
        /// 配置角色权限
        /// </summary>
        /// <param name="param">
        /// {
        ///     "OpenId": "9e26f6e2244f7e34ed8b4eabdcb2f9c4","RoleId": "1","PageIds": "1,2,3"
        /// }
        /// </param>
        /// <returns></returns>
        [HttpPost]
        public MsgInfo<List<AdminPower>> PostPowerByRole(dynamic param)
        {
            MsgInfo<List<AdminPower>> info = new();
            DateTime t = DateTime.Now;
            var data = JsonConvert.DeserializeObject<dynamic>(param.ToString());
            string OpenId = data.OpenId.ToString();
            if (Object.ReferenceEquals(null, data))
            {
                info.code = 400;
                info.msg = "Network Exception";
                return info;
            }
            long _PassportId = IsLogin(OpenId);
            if (_PassportId <= 0)
            {
                info.code = 400;
                info.msg = "Please login";
                return info;
            }
            int RoleId = Convert.ToInt32(data.RoleId.ToString());
            string PageIds = data.PageIds.ToString();
            List<AdminPower> list_power = db.Queryable<AdminPower>().Where(c => c.RoleId == RoleId).ToList();
            if (list_power.Count > 0)
            {
                int dele = db.Deleteable<AdminPower>().Where(c => c.RoleId == RoleId).ExecuteCommand();
                if (dele < 1)
                {
                    info.code = 400;
                    info.msg = "Network Exception";
                    return info;
                }
            }

            int[] IntPageIds = Array.ConvertAll<string, int>(PageIds.Split(new char[] { ',' }, StringSplitOptions.RemoveEmptyEntries), s => int.Parse(s));
            List<AdminPower> PowerList = new List<AdminPower>();
            for (int n = 0; n < IntPageIds.Length; n++)
            {
                int PageId = Convert.ToInt32(IntPageIds[n].ToString());
                AdminPower power = new()
                {
                    AddTime = t,
                    UpdateTime = t,
                    IsValid = 1,
                    Sort = 1,
                    Ip = YRHelper.GetClientIPAddress(HttpContext),

                    RoleId = RoleId,
                    PageId = PageId,
                    ManageUserPassportId = _PassportId
                };
                PowerList.Add(power);
            }
            db.Insertable(PowerList).ExecuteCommand();

            info.code = 200;
            info.msg = "success";

            return info;
        }
        #endregion

        [HttpGet]
        public async Task<MsgInfo<string>> RefenOpenId(string OpenId,string UserType ,string ChildOpenId)
        {
            MsgInfo<string> info = new();
            List<string> array = new List<string>() { OpenId, ChildOpenId };
            long _PassportId = IsLogin(OpenId, UserType);
            if (_PassportId <= 0)
            {
                info.code = 400;
                info.msg = "Please login";
                return info;
            }
            var adminuser = await db.Queryable<AdminUser>().Where(x => array.Contains(x.OpenId)).ToListAsync();
            var my = adminuser.FirstOrDefault(x => x.OpenId == OpenId);
            var child = adminuser.FirstOrDefault(x => x.OpenId == ChildOpenId);
        

                string Newopenid = string.Empty;
                if (child.UpdateTime > DateTime.Now)
                {
                    Newopenid = child.OpenId;
                }
                else
                {
                    Newopenid = YRHelper.get_md5_32(CreateOpenId());
                    var dic = new Dictionary<string, object>();
                    dic.Add("Id", child.Id);
                    dic.Add("OpenId", Newopenid);
                    dic.Add("UpdateTime", DateTime.Now.AddMinutes(60));
                    db.Updateable<AdminUser>(dic).WhereColumns("Id").ExecuteCommand();
                }
                info.data = Newopenid;
            
            return info;
        }

        /// <summary>
        /// 获取权限(UserType:0为管理员用户，1为会员用户)
        /// </summary>
        /// <param name="OpenId">OpenId</param>
        /// <param name="UserType">(UserType:0为管理员用户，1为会员用户)</param>
        /// <returns></returns>
        [HttpGet]
        public async Task<MsgInfo<List<RetuAdminPage>>> GetPowerGo(string OpenId, string UserType, string ChildOpenId)
        {
            MsgInfo<List<RetuAdminPage>> info = new();
     
            long _PassportId = IsLogin(OpenId, UserType);
            if (_PassportId <= 0)
            {
                info.code = 400;
                info.msg = "Please login";
                return info;
            }
            List<string> array = new List<string>() { OpenId, ChildOpenId };
            var adminuser = await db.Queryable<AdminUser>().Where(x => array.Contains(x.OpenId)).ToListAsync();
            var my = adminuser.FirstOrDefault(x => x.OpenId == OpenId);
            var child = adminuser.FirstOrDefault(x => x.OpenId == ChildOpenId);
            if (my?.RoleId <= child?.RoleId && my?.RoleId < 3)
            {

                string Newopenid = string.Empty;
                if (child.UpdateTime >= DateTime.Now)
                {
                    Newopenid = child.OpenId;
                }
                else
                {
                    Newopenid = YRHelper.get_md5_32(OpenId);
                    var dic = new Dictionary<string, object>();
                    dic.Add("Id", child.Id);
                    dic.Add("OpenId", Newopenid);
                    dic.Add("UpdateTime", DateTime.Now.AddMinutes(60));
                    db.Updateable<AdminUser>(dic).WhereColumns("Id").ExecuteCommand();
                }
                List<RetuAdminPage> allPages = db.Queryable<AdminPage>()
                         .LeftJoin<AdminPower>((o, i) => o.Id == i.PageId)
                         .Where((o, i) => i.RoleId == child.RoleId)
                         .Select(o => new RetuAdminPage
                         {
                             Id = o.Id,
                             PageName = o.PageName,
                             PageUrl = o.PageUrl,
                             PageIcon = o.PageIcon,
                             icon = o.PageIcon,
                             ParentId = o.ParentId,
                             IsGoogle= (int)child.IsGoogle
                         })
                         .ToList();
                List<RetuAdminPage> pages = db.Queryable<AdminPage>()
                .LeftJoin<AdminPower>((o, i) => o.Id == i.PageId)
                .Where((o, i) => i.RoleId == child.RoleId && o.ParentId == 0)
                .Select(o => new RetuAdminPage
                {
                    Id = o.Id,
                    PageName = o.PageName,
                    PageUrl = o.PageUrl,
                    PageIcon = o.PageIcon,
                    icon = o.PageIcon,
                    ParentId = o.ParentId,
                    OpenId = Newopenid
                })
                .ToList();

                pages.ForEach(x => x.Childrens = allPages.Where(c => c.ParentId == x.Id).Select(o => new RetuAdminPage()
                {
                    Id = o.Id,
                    PageName = o.PageName,
                    PageUrl = o.PageUrl,
                    PageIcon = o.PageIcon,
                    icon = o.PageIcon,
                    ParentId = o.ParentId,
                    OpenId = Newopenid,
                    IsGoogle = (int)child.IsGoogle
                }).ToList());

                pages = (from c in pages orderby c.Id select c).ToList();

                info.code = 200;
                info.msg = "success";
                info.data = pages;

            }
            else
            {
                info.msg = "You don't have access.";
            }
            return info;
        }



        /// <summary>
        /// 管理员/会员密码修改(UserType:0为管理员用户，1为会员用户)
        /// </summary>
        /// <param name="param">
        /// {
        ///     "OpenId": "9e26f6e2244f7e34ed8b4eabdcb2f9c4","OldPwd": "123456","NewPwd": "123456","ConfimNewPwd": "123456","UserType":"0"
        /// }
        /// </param>
        /// <returns></returns>
        [HttpPost]
        public MsgInfo<string> PostPwd(dynamic param)
        {
            MsgInfo<string> info = new();
            DateTime t = DateTime.Now;
            var data = JsonConvert.DeserializeObject<dynamic>(param.ToString());
            string OpenId = data.OpenId.ToString();
            if (Object.ReferenceEquals(null, data))
            {
                info.code = 400;
                info.msg = "Network Exception";
                return info;
            }
            long _PassportId = IsLogin(OpenId);
            if (_PassportId <= 0)
            {
                info.code = 400;
                info.msg = "Please login";
                return info;
            }
            string OldPwd = data.OldPwd.ToString();
            string NewPwd = data.NewPwd.ToString();
            string ConfimNewPwd = data.ConfimNewPwd.ToString();
            string UserType = data.UserType.ToString();
            if (NewPwd != ConfimNewPwd)
            {
                info.code = 400;
                info.msg = "The new password is inconsistent";
                return info;
            }

            if (UserType == "0")
            {
                AdminUser admin = db.Queryable<AdminUser>().First(c => c.PassportId == _PassportId && c.Pwd == YRHelper.get_md5_32(OldPwd));
                if (admin == null)
                {
                    info.code = 400;
                    info.msg = "OldPwd's wrong";
                    return info;
                }

                int i = db.Updateable<AdminUser>().
                    SetColumns(c => c.UpdateTime == t).
                    SetColumns(c => c.Pwd == YRHelper.get_md5_32(NewPwd)).
                    Where(c => c.PassportId == admin.PassportId).
                    ExecuteCommand();
                if (i < 1)
                {
                    info.code = 400;
                    info.msg = "Network Exception";
                    return info;
                }
                info.code = 200;
                info.msg = "success";
            }
            else if (UserType == "1")
            {
                UserInfo admin = db.Queryable<UserInfo>().First(c => c.PassportId == _PassportId && c.Pwd == YRHelper.get_md5_32(OldPwd));
                if (admin == null)
                {
                    info.code = 400;
                    info.msg = "OldPwd's wrong";
                    return info;
                }

                int i = db.Updateable<UserInfo>().
                    SetColumns(c => c.UpdateTime == t).
                    SetColumns(c => c.Pwd == YRHelper.get_md5_32(NewPwd)).
                    Where(c => c.PassportId == admin.PassportId).
                    ExecuteCommand();
                if (i < 1)
                {
                    info.code = 400;
                    info.msg = "Network Exception";
                    return info;
                }
                info.code = 200;
                info.msg = "success";
            }

            return info;
        }


        /// <summary>
        /// 获取会员列表
        /// </summary>
        /// <param name="OpenId">OpenId</param>
        /// <param name="UserType">(UserType:0为管理员用户，1为会员用户)</param>
        /// <param name="HashAddress">哈希地址</param>
        /// <param name="UserName">用户名</param>
        /// <param name="IsValid">状态(0无效 1有效)</param>
        /// <param name="AddTime1">注册时间1</param>
        /// <param name="AddTime2">注册时间2</param>
        /// <param name="PageIndex">当前页</param>
        /// <param name="PageSize">每页数量</param>
        /// <returns></returns>
        [HttpGet]
        public MsgInfo<List<RetuUserInfo>> GetMember(string OpenId, string UserType, string HashAddress, string UserName, string IsValid, 
            string AddTime1, string AddTime2, int PageIndex, int PageSize = 20)
        {
            MsgInfo<List<RetuUserInfo>> info = new();
            long _PassportId = IsLogin(OpenId, UserType);
            if (_PassportId <= 0)
            {
                info.code = 400;
                info.msg = "Please login";
                return info;
            }

            int RoleId = GetRoleId(_PassportId, UserType);
            List<UserInfo> userInfos = GetMyChildsAllT<UserInfo>("UserInfo", "", _PassportId, UserType);
            if (RoleId == 1)
            {
                userInfos = db.Queryable<UserInfo>().ToList();
            }
            int _IsValid = 1;
            int.TryParse(IsValid, out _IsValid);

            List<RetuUserInfo> list = (from c in userInfos
                                       select new RetuUserInfo
                                       {
                                           Id = c.Id,
                                           AddTime = c.AddTime,
                                           UpdateTime = c.UpdateTime,
                                           IsValid = c.IsValid,
                                           Sort = c.Sort,
                                           Ip = c.Ip,

                                           PassportId = c.PassportId,
                                           HashAddress = c.HashAddress,
                                           UserName = c.UserName,
                                           TJCode = c.TJCode,
                                           ParentId = c.ParentId,
                                           ManageUserPassportId = c.ManageUserPassportId,
                                           RoleId = c.RoleId,
                                           OpenId = c.OpenId,
                                           UserLang = c.UserLang,
                                           LoginErrorCount = c.LoginErrorCount,
                                           OtherMsg = JsonConvert.DeserializeObject<RetuOtherMsg>(c.OtherMsg ?? "{}"),
                                           RoleName = c.RoleId == 4 ? "代理" : db.Queryable<AdminRole>().First(x => x.Id == c.RoleId)?.RoleName ?? "",
                                           AgentTypeID = c.AgentTypeID,
                                           AgentTypeName = db.Queryable<AgentType>().First(x => x.Id == c.AgentTypeID)?.AgentName ?? "",
                                       }).ToList();

            DateTime t1 = DateTime.Now;
            DateTime.TryParse(AddTime1, out t1);
            DateTime t2 = DateTime.Now;
            DateTime.TryParse(AddTime2, out t2);
            if (!string.IsNullOrWhiteSpace(HashAddress))
            {
                list = (from c in list where c.HashAddress == HashAddress select c).ToList();
            }
            if (!string.IsNullOrWhiteSpace(UserName))
            {
                list = (from c in list where c.UserName == UserName select c).ToList();
            }
            if (!string.IsNullOrWhiteSpace(IsValid))
            {
                list = (from c in list where c.IsValid == _IsValid select c).ToList();
            }
            //时间
            if (!string.IsNullOrWhiteSpace(AddTime1) && !string.IsNullOrWhiteSpace(AddTime2))
            {
                list = (from c in list where c.AddTime >= t1 && c.AddTime <= t2 select c).ToList();
            }
            else if (!string.IsNullOrWhiteSpace(AddTime1))
            {
                list = (from c in list where c.AddTime >= t1 select c).ToList();
            }
            else if (!string.IsNullOrWhiteSpace(AddTime2))
            {
                list = (from c in list where c.AddTime <= t2 select c).ToList();
            }
            info.data_count = list.Count;
            if (PageIndex > 0)
            {
                list = (from c in list orderby c.AddTime descending select c).Skip((PageIndex - 1) * PageSize).Take(PageSize).ToList();
            }

            for (int i = 0; i < list.Count; i++)
            {
                list[i].Pwd = "";
            }
            info.code = 200;
            info.msg = "success";
            info.data = list;
            return info;
        }

        /// <summary>
        /// 获取代理列表
        /// </summary>
        /// <param name="OpenId">OpenId</param>
        /// <param name="UserType">(UserType:0为管理员用户，1为会员用户)</param>
        /// <param name="HashAddress">哈希地址</param>
        /// <param name="UserName">用户名</param>
        /// <param name="IsValid">状态(0无效 1有效)</param>
        /// <param name="AddTime1">注册时间1</param>
        /// <param name="AddTime2">注册时间2</param>
        /// <param name="PageIndex">当前页</param>
        /// <param name="PageSize">每页数量</param>
        /// <returns></returns>
        [HttpGet]
        public MsgInfo<List<RetuUserInfo>> GetAgent(string OpenId, string UserType, string HashAddress, string UserName, string IsValid, 
            string AddTime1, string AddTime2, int PageIndex, int PageSize = 20)
        {
            MsgInfo<List<RetuUserInfo>> info = new();
            long _PassportId = IsLogin(OpenId, UserType);
            if (_PassportId <= 0)
            {
                info.code = 400;
                info.msg = "Please login";
                return info;
            }

            int RoleId = GetRoleId(_PassportId, UserType);
            List<UserInfo> userInfos = GetMyChildsAllT<UserInfo>("UserInfo", " and RoleId = 4", _PassportId, UserType);
            if (RoleId == 1)
            {
                userInfos = db.Queryable<UserInfo>().ToList();
            }
            int _IsValid = 1;
            int.TryParse(IsValid, out _IsValid);

            List<RetuUserInfo> list = (from c in userInfos where c.RoleId == 4
                                       select new RetuUserInfo
                                       {
                                           Id = c.Id,
                                           AddTime = c.AddTime,
                                           UpdateTime = c.UpdateTime,
                                           IsValid = c.IsValid,
                                           Sort = c.Sort,
                                           Ip = c.Ip,

                                           PassportId = c.PassportId,
                                           HashAddress = c.HashAddress,
                                           UserName = c.UserName,
                                           TJCode = c.TJCode,
                                           ParentId = c.ParentId,
                                           ManageUserPassportId = c.ManageUserPassportId,
                                           RoleId = c.RoleId,
                                           OpenId = c.OpenId,
                                           UserLang = c.UserLang,
                                           LoginErrorCount = c.LoginErrorCount,
                                           OtherMsg = JsonConvert.DeserializeObject<RetuOtherMsg>(c.OtherMsg ?? "{}"),
                                           RoleName = c.RoleId == 4 ? "代理" : db.Queryable<AdminRole>().First(x => x.Id == c.RoleId)?.RoleName ?? "",
                                           AgentTypeID = c.AgentTypeID,
                                           AgentTypeName = db.Queryable<AgentType>().First(x => x.Id == c.AgentTypeID)?.AgentName ?? "",
                                           AgentPopIsRebate = (db.Queryable<AgentDetails>().First(x => x.AgentId == c.AgentTypeID && x.PassportId == c.PassportId)?.AgentPopIsRebate.ToString() ?? "").Length > 0 ? "%" : ""
                                       }).ToList();


            DateTime t1 = DateTime.Now;
            DateTime.TryParse(AddTime1, out t1);
            DateTime t2 = DateTime.Now;
            DateTime.TryParse(AddTime2, out t2);
            if (!string.IsNullOrWhiteSpace(HashAddress))
            {
                list = (from c in list where c.HashAddress == HashAddress select c).ToList();
            }
            if (!string.IsNullOrWhiteSpace(UserName))
            {
                list = (from c in list where c.UserName == UserName select c).ToList();
            }
            if (!string.IsNullOrWhiteSpace(IsValid))
            {
                list = (from c in list where c.IsValid == _IsValid select c).ToList();
            }
            //时间
            if (!string.IsNullOrWhiteSpace(AddTime1) && !string.IsNullOrWhiteSpace(AddTime2))
            {
                list = (from c in list where c.AddTime >= t1 && c.AddTime <= t2 select c).ToList();
            }
            else if (!string.IsNullOrWhiteSpace(AddTime1))
            {
                list = (from c in list where c.AddTime >= t1 select c).ToList();
            }
            else if (!string.IsNullOrWhiteSpace(AddTime2))
            {
                list = (from c in list where c.AddTime <= t2 select c).ToList();
            }
            info.data_count = list.Count;
            if (PageIndex > 0)
            {
                list = (from c in list orderby c.AddTime descending select c).Skip((PageIndex - 1) * PageSize).Take(PageSize).ToList();
            }

            info.code = 200;
            info.msg = "success";
            info.data = list;
            return info;
        }

        /// <summary>
        /// 编辑会员
        /// </summary>
        /// <param name="param">
        /// {
        ///     "OpenId": "9e26f6e2244f7e34ed8b4eabdcb2f9c4","PassportId":"111","IsValid":"会员状态[1]开启 / [0]禁用",
        ///     "PopVaild":"推广资格[1]开启 / [0]禁用","Remarks":"备注"
        /// }
        /// </param>
        /// <returns></returns>
        [HttpPost]
        public MsgInfo<string> PostMember(dynamic param)
        {
            MsgInfo<string> info = new();
            DateTime t = DateTime.Now;
            var data = JsonConvert.DeserializeObject<dynamic>(param.ToString());
            string OpenId = data.OpenId.ToString();
            string IsValid = data.IsValid.ToString();
            string PassportId = data.PassportId.ToString();
            string PopVaild = data.PopVaild.ToString();
            string Remarks = data.Remarks.ToString();

            if (Object.ReferenceEquals(null, data))
            {
                info.code = 400;
                info.msg = "Network Exception";
                return info;
            }
            long _PassportId = IsLogin(OpenId);
            if (_PassportId <= 0)
            {
                info.code = 400;
                info.msg = "Please login";
                return info;
            }
            long UserPassportId = 0;
            long.TryParse(PassportId, out UserPassportId);
            long _IsValid = 0;
            long.TryParse(IsValid, out _IsValid);

            //"完成会员用户信息的编辑、操作
            //用户名编辑、会员状态开启 / 禁用、推广资格开启 / 禁用"

            UserInfo user = db.Queryable<UserInfo>().First(c => c.PassportId == UserPassportId);
            if (user == null)
            {
                info.code = 400;
                info.msg = "Network Exception";
                return info;
            }
            RetuOtherMsg retuOtherMsg = JsonConvert.DeserializeObject<RetuOtherMsg>(user.OtherMsg);
            if (retuOtherMsg != null)
            {
                retuOtherMsg.PopVaild = PopVaild;
                retuOtherMsg.Remarks = Remarks;
            }

            int i = db.Updateable<UserInfo>().
                SetColumns(c => c.UpdateTime == t).
                SetColumns(c => c.IsValid == _IsValid).
                SetColumns(c => c.OtherMsg == JsonConvert.SerializeObject(retuOtherMsg)).
                Where(c => c.PassportId == UserPassportId).
                ExecuteCommand();
            if (i < 1)
            {
                info.code = 400;
                info.msg = "Network Exception";
                return info;
            }
            info.code = 200;
            info.msg = "success";

            return info;
        }


        #region 游戏产品
        /// <summary>
        /// 获取游戏列表
        /// </summary>
        /// <param name="OpenId">OpenId</param>
        /// <param name="UserType">(UserType:0为管理员用户，1为会员用户)</param>
        /// <param name="GameName">游戏名称</param>
        /// <param name="PageIndex">当前页</param>
        /// <param name="PageSize">每页数量</param>
        /// <returns></returns>
        [HttpGet]
        public MsgInfo<List<GameType>> GetGame(string OpenId, string UserType, string GameName, int PageIndex, int PageSize = 20)
        {
            MsgInfo<List<GameType>> info = new();
            long _PassportId = IsLogin(OpenId, UserType);
            if (_PassportId <= 0)
            {
                info.code = 400;
                info.msg = "Please login";
                return info;
            }

            if (UserType != "0")
            {
                info.code = 200;
                info.msg = "";
                return info;
            }

            AdminUser users = db.Queryable<AdminUser>().First(c => c.PassportId == _PassportId);
            if (users == null)
            {
                info.code = 400;
                info.msg = "Network Exception";
                return info;
            }

            List<GameType> list = db.Queryable<GameType>().ToList();
            if (users.RoleId != 1)
            {
                list = db.Queryable<GameType>().InnerJoin<GameDetails>((c, d) => c.Id == d.GameTypeId).Where((c, d) => d.ManageUserPassportId == _PassportId).Select((c, d) => new GameType
                {
                    Id = c.Id,
                    AddTime = c.AddTime,
                    UpdateTime = c.UpdateTime,
                    IsValid = c.IsValid,
                    Sort = c.Sort,
                    Ip = c.Ip,

                    GameName = c.GameName,
                    GameDesc = c.GameDesc
                }).ToList();
            }
            if (!string.IsNullOrWhiteSpace(GameName))
            {
                list = (from c in list where c.GameName == GameName select c).ToList();
            }
            info.data_count = list.Count;
            if (PageIndex > 0)
            {
                list = (from c in list orderby c.AddTime descending select c).Skip((PageIndex - 1) * PageSize).Take(PageSize).ToList();
            }

            info.code = 200;
            info.msg = "success";
            info.data = list;
            return info;
        }

        /// <summary>
        /// 获取游戏详情列表
        /// </summary>
        /// <param name="OpenId">OpenId</param>
        /// <param name="PageIndex">当前页</param>
        /// <param name="PageSize">每页数量</param>
        /// <returns></returns>
        [HttpGet]
        public MsgInfo<List<RetuGameDetails>> GetGameDetails(string OpenId, int PageIndex, int PageSize = 20)
        {
            MsgInfo<List<RetuGameDetails>> info = new();
            long _PassportId = IsLogin(OpenId);
            if (_PassportId <= 0)
            {
                info.code = 400;
                info.msg = "Please login";
                return info;
            }

            List<AdminGameRelation> adminGameRelations = db.Queryable<AdminGameRelation>().Where(c => c.PassportId == _PassportId).ToList();
            List<RetuGameDetails> list = db.Queryable<GameDetails>().
                LeftJoin<GameType>((c, d) => c.GameTypeId == d.Id).
                Where(c => c.ManageUserPassportId == _PassportId).
                Select((c, d) => new RetuGameDetails()
                {
                    Id = c.Id,
                    AddTime = c.AddTime,
                    UpdateTime = c.UpdateTime,
                    IsValid = c.IsValid,
                    Sort = c.Sort,
                    Ip = c.Ip,

                    ManageUserPassportId = c.ManageUserPassportId,
                    GameName = d.GameName,
                    GameDesc = d.GameDesc,
                    //Create = db.Queryable<AdminUser>().First(m => m.PassportId == d.PassportId).UserName

                    GameTypeId = c.GameTypeId,
                    Odds = c.Odds,
                    Odds0 = c.Odds0,
                    Odds1 = c.Odds1,
                    Odds2 = c.Odds2,
                    Odds3 = c.Odds3,
                    Odds4 = c.Odds4,
                    Odds5 = c.Odds5,
                    Odds6 = c.Odds6,
                    Odds7 = c.Odds7,
                    Odds8 = c.Odds8,
                    Odds9 = c.Odds9,
                    GameFee = c.GameFee,
                    InvalidGameFee = c.InvalidGameFee,
                    MinQuota = c.MinQuota,
                    MaxQuota = c.MaxQuota,
                    GameDetsils = c.GameDetsils
                }).
                ToList();
            list.ForEach(c => c.HashAddress = adminGameRelations.Where(d => d.GameDetailsId == c.GameTypeId).Select(d => d.HashAddress).FirstOrDefault());

            info.data_count = list.Count;
            if (PageIndex > 0)
            {
                list = (from c in list orderby c.AddTime descending select c).Skip((PageIndex - 1) * PageSize).Take(PageSize).ToList();
            }

            info.code = 200;
            info.msg = "success";
            info.data = list;
            return info;
        }

        /// <summary>
        /// 添加/修改游戏(AgentId:""为添加，有值为修改)
        /// </summary>
        /// <param name="param">{"OpenId":"97c2ede4a490112efeb65aecd8ecaafe","GameName":"单双","GameDesc":"","GameId":""}</param>
        /// <returns></returns>
        [HttpPost]
        public MsgInfo<string> PostGame(dynamic param)
        {
            MsgInfo<string> info = new();
            DateTime t = DateTime.Now;
            var data = JsonConvert.DeserializeObject<dynamic>(param.ToString());
            string OpenId = data.OpenId.ToString();
            if (Object.ReferenceEquals(null, data))
            {
                info.code = 400;
                info.msg = "Network Exception";
                return info;
            }
            long _PassportId = IsLogin(OpenId);
            if (_PassportId <= 0)
            {
                info.code = 400;
                info.msg = "Please login";
                return info;
            }

            string GameName = data.GameName.ToString();
            string GameDesc = data.GameDesc.ToString();
            string GameId = data.GameId.ToString();
            int _GameId = 0;
            int.TryParse(GameId, out _GameId);
            if (string.IsNullOrWhiteSpace(GameId))
            {
                GameType game = new()
                {
                    AddTime = t,
                    UpdateTime = t,
                    IsValid = 1,
                    Sort = 1,
                    Ip = YRHelper.GetClientIPAddress(HttpContext),

                    GameName = GameName,
                    GameDesc = GameDesc
                };
                int i = db.Insertable(game).ExecuteCommand();
                if (i < 1)
                {
                    info.code = 400;
                    info.msg = "Network Exception";
                    return info;
                }
            }
            else
            {
                int i = db.Updateable<GameType>().
            SetColumns(c => c.UpdateTime == t).
            SetColumns(c => c.GameName == GameName).
            SetColumns(c => c.GameDesc == GameDesc).
            Where(c => c.Id == _GameId).
            ExecuteCommand();
                if (i < 1)
                {
                    info.code = 400;
                    info.msg = "Network Exception";
                    return info;
                }
            }


            info.code = 200;
            info.msg = "success";

            return info;
        }

        /// <summary>
        /// 更新游戏详情
        /// </summary>
        /// <param name="param">
        /// {"OpenId":"9e26f6e2244f7e34ed8b4eabdcb2f9c4","IsValid":"(0无效 1有效)","CoinType":"币种类别","HashAddress":"钱包地址",
        /// "GameTypeDetailsId":4,"GameName":"哈希游戏","Odds":1,"Odds0":1,"Odds1":1,"Odds2":1,"Odds3":1,"Odds4":1,"Odds5":1,"Odds6":1,"Odds7":1,
        /// "Odds8":1,"Odds9":1,"MinQuota":1,"MaxQuota":1,"MinQuota1":1,"MaxQuota1":1,"GameFee":"抽水比例","InvalidGameFee":"无效投注费率","GameDetsils":"描述",GoogleCode}
        /// </param>
        /// <returns></returns>
        [HttpPost]
        public MsgInfo<string> PostUpdateGameDetails(dynamic param)
        {
            MsgInfo<string> info = new();
            DateTime t = DateTime.Now;
            var data = JsonConvert.DeserializeObject<dynamic>(param.ToString());
            string OpenId = data.OpenId.ToString();
            if (Object.ReferenceEquals(null, data))
            {
                info.code = 400;
                info.msg = "Network Exception";
                return info;
            }
            long _PassportId = IsLogin(OpenId);
            if (_PassportId <= 0)
            {
                info.code = 400;
                info.msg = "Please login";
                return info;
            }
            string GoogleCode = data.GoogleCode.ToString();
            var mys = db.Queryable<AdminUser>().First(d => d.OpenId == OpenId);
                if (mys.IsGoogle == 1)
                    if (!GoogleHelp.CheckCode(GoogleCode, mys.GoogleKey))
                    {
                        info.code = 400;
                        info.msg = "Verification code error";
                        return info;
                    }
            
            string IsValid = data.IsValid.ToString();
            int GameTypeDetailsId = int.Parse(data.GameTypeDetailsId.ToString());
            string HashAddress = data.HashAddress.ToString();
            string CoinType = data.CoinType.ToString();
            string GameName = data.GameName.ToString();
            decimal Odds = decimal.Parse(data.Odds.ToString());
            decimal Odds0 = decimal.Parse(data.Odds0.ToString());
            decimal Odds1 = decimal.Parse(data.Odds1.ToString());
            decimal Odds2 = decimal.Parse(data.Odds2.ToString());
            decimal Odds3 = decimal.Parse(data.Odds3.ToString());
            decimal Odds4 = decimal.Parse(data.Odds4.ToString());
            decimal Odds5 = decimal.Parse(data.Odds5.ToString());
            decimal Odds6 = decimal.Parse(data.Odds6.ToString());
            decimal Odds7 = decimal.Parse(data.Odds7.ToString());
            decimal Odds8 = decimal.Parse(data.Odds8.ToString());
            decimal Odds9 = decimal.Parse(data.Odds9.ToString());
            decimal GameFee = decimal.Parse(data.GameFee.ToString());
            decimal InvalidGameFee = decimal.Parse(data.InvalidGameFee.ToString());
            decimal MinQuota = decimal.Parse(data.MinQuota.ToString());
            decimal MaxQuota = decimal.Parse(data.MaxQuota.ToString());
            decimal MinQuota1 = decimal.Parse(data.MinQuota1.ToString());
            decimal MaxQuota1 = decimal.Parse(data.MaxQuota1.ToString());
            string GameDetsils = data.GameDetsils.ToString();
            long ManageUserPassportId = _PassportId;
            int _IsValid = 1;
            int.TryParse(IsValid, out _IsValid);
            int _CoinType = 0;
            int.TryParse(CoinType, out _CoinType);

            //var relations = db.Queryable<AdminGameRelation>().First(c => c.HashAddress == HashAddress);
            //if (relations!=null)
            //{
            //    info.code = 400;
            //    info.msg = "HashAddress already exists";
            //    return info;
            //}

            //更新hash收款地址
            List<SugarParameter> list_relation = new List<SugarParameter>();
            list_relation.Add(new SugarParameter("@updatetime", t));
            list_relation.Add(new SugarParameter("@HashAddress", HashAddress));
            list_relation.Add(new SugarParameter("@CoinType", _CoinType));
            list_relation.Add(new SugarParameter("@PassportId", _PassportId));
            list_relation.Add(new SugarParameter("@GameDetailsId", GameTypeDetailsId));
            string sql_update_relation = string.Format(@"update AdminGameRelation set updatetime=@updatetime,HashAddress=@HashAddress,CoinType=@CoinType where PassportId=@PassportId and GameDetailsId=@GameDetailsId");
            int m = db.Ado.ExecuteCommand(sql_update_relation, list_relation);
            if (m < 1)
            {
                info.code = 400;
                info.msg = "Network Exception";
                return info;
            }

            List<SugarParameter> list = new List<SugarParameter>();
            list.Add(new SugarParameter("@updatetime", t));
            list.Add(new SugarParameter("@GameName", GameName));
            list.Add(new SugarParameter("@Odds", Odds));
            list.Add(new SugarParameter("@Odds0", Odds0));
            list.Add(new SugarParameter("@Odds1", Odds1));
            list.Add(new SugarParameter("@Odds2", Odds2));
            list.Add(new SugarParameter("@Odds3", Odds3));
            list.Add(new SugarParameter("@Odds4", Odds4));
            list.Add(new SugarParameter("@Odds5", Odds5));
            list.Add(new SugarParameter("@Odds6", Odds6));
            list.Add(new SugarParameter("@Odds7", Odds7));
            list.Add(new SugarParameter("@Odds8", Odds8));
            list.Add(new SugarParameter("@Odds9", Odds9));
            list.Add(new SugarParameter("@GameFee", GameFee));
            list.Add(new SugarParameter("@InvalidGameFee", InvalidGameFee));
            list.Add(new SugarParameter("@MinQuota", MinQuota));
            list.Add(new SugarParameter("@MaxQuota", MaxQuota));
            list.Add(new SugarParameter("@MinQuota1", MinQuota1));
            list.Add(new SugarParameter("@MaxQuota1", MaxQuota1));
            list.Add(new SugarParameter("@GameDetsils", GameDetsils));
            list.Add(new SugarParameter("@IsValid", _IsValid));
            list.Add(new SugarParameter("@PassportId", _PassportId));
            list.Add(new SugarParameter("@GameDetailsId", GameTypeDetailsId));
            string sql_update = string.Format(@"
                update GameDetails set updatetime=@updatetime,GameName=@GameName,Odds=@Odds, 
                    Odds0=@Odds0,Odds1=@Odds1,Odds2=@Odds2,Odds3=@Odds3,Odds4=@Odds4,Odds5=@Odds5,Odds6=@Odds6,Odds7=@Odds7,Odds8=@Odds8,Odds9=@Odds9,
                    GameFee=@GameFee,InvalidGameFee=@InvalidGameFee,MinQuota=@MinQuota,MaxQuota=@MaxQuota,MinQuota1=@MinQuota1,MaxQuota1=@MaxQuota1,GameDetsils=@GameDetsils,IsValid=@IsValid 
                    where ManageUserPassportId=@PassportId and GameTypeId=@GameDetailsId");
            int n = db.Ado.ExecuteCommand(sql_update, list);

            if (n < 1)
            {
                info.code = 400;
                info.msg = "Network Exception";
                return info;
            }

            info.code = 200;
            info.msg = "success";

            return info;
        }

        /// <summary>
        /// 获取币种类型
        /// </summary>
        /// <param name="OpenId"></param>
        /// <param name="PageIndex"></param>
        /// <param name="PageSize"></param>
        /// <returns></returns>
        [HttpGet]
        public MsgInfo<List<CoinType>> GetCoinType(string OpenId, int PageIndex, int PageSize = 20)
        {
            MsgInfo<List<CoinType>> info = new();
            long _PassportId = IsLogin(OpenId);
            if (_PassportId <= 0)
            {
                info.code = 400;
                info.msg = "Please login";
                return info;
            }

            List<CoinType> list = db.Queryable<CoinType>().Where(c => c.IsValid == 1).ToList();
            info.data_count = list.Count;
            if (PageIndex > 0)
            {
                list = (from c in list orderby c.AddTime descending select c).Skip((PageIndex - 1) * PageSize).Take(PageSize).ToList();
            }

            info.code = 200;
            info.msg = "success";
            info.data = list;
            return info;
        }

        /// <summary>
        /// 添加/修改币种类型(添加CoinId为空)
        /// </summary>
        /// <param name="param">
        /// {"OpenId":"9e26f6e2244f7e34ed8b4eabdcb2f9c4","CoinName":"币种名称","CoinKey":"键","CoinValue":"值","CoinDesc":"描述","CoinId":""}
        /// </param>
        /// <returns></returns>
        [HttpPost]
        public MsgInfo<string> PostCoinType(dynamic param)
        {
            MsgInfo<string> info = new();
            DateTime t = DateTime.Now;
            var data = JsonConvert.DeserializeObject<dynamic>(param.ToString());
            string OpenId = data.OpenId.ToString();
            if (Object.ReferenceEquals(null, data))
            {
                info.code = 400;
                info.msg = "Network Exception";
                return info;
            }
            long CurrentPassportId = IsLogin(OpenId);
            if (CurrentPassportId <= 0)
            {
                info.code = 400;
                info.msg = "Please login";
                return info;
            }

            int MyRoleId = GetRoleId(CurrentPassportId);
            if (MyRoleId != 1)
            {
                info.code = 400;
                info.msg = "no permission";
                return info;
            }

            string CoinName = data.CoinName.ToString();
            string CoinKey = data.CoinKey.ToString();
            string CoinValue = data.CoinValue.ToString();
            string CoinDesc = data.CoinDesc.ToString();
            string CoinId = data.CoinId.ToString();
            if (string.IsNullOrEmpty(CoinId))
            {
                CoinType role = new()
                {
                    AddTime = t,
                    UpdateTime = t,
                    IsValid = 1,
                    Sort = 1,
                    Ip = YRHelper.GetClientIPAddress(HttpContext),

                    CoinName = CoinName,
                    CoinKey = CoinKey,
                    CoinValue = CoinValue,
                    CoinDesc = CoinDesc,
                };
                int m = db.Insertable(role).ExecuteCommand();
                if (m < 1)
                {
                    info.code = 400;
                    info.msg = "Network Exception";
                    return info;
                }
            }
            else
            {
                int _CoinId = 0;
                int.TryParse(CoinId, out _CoinId);
                int m = db.Updateable<CoinType>().SetColumns(c => c.UpdateTime == t).
                    SetColumns(c => c.CoinName == CoinName).
                    SetColumns(c => c.CoinKey == CoinKey).
                    SetColumns(c => c.CoinValue == CoinValue).
                    SetColumns(c => c.CoinDesc == CoinDesc).
                    Where(c => c.Id == _CoinId).ExecuteCommand();
                if (m < 1)
                {
                    info.code = 400;
                    info.msg = "Network Exception";
                    return info;
                }
            }


            info.code = 200;
            info.msg = "success";

            return info;
        }
        #endregion

        #region 代理类型
        /// <summary>
        /// 获取代理类型列表
        /// </summary>
        /// <param name="OpenId"></param>
        /// <param name="AgentTypeName">代理类型名称</param>
        /// <param name="PageIndex"></param>
        /// <param name="PageSize"></param>
        /// <returns></returns>
        [HttpGet]
        public MsgInfo<List<AgentType>> GetAgentType(string OpenId, string AgentTypeName, int PageIndex, int PageSize = 20, string UserType = "0")
        {
            MsgInfo<List<AgentType>> info = new();
            long _PassportId = IsLogin(OpenId, UserType);
            if (_PassportId <= 0)
            {
                info.code = 400;
                info.msg = "Please login";
                return info;
            }

            List<AgentType> list = db.Queryable<AgentType>().ToList();
            int _RoleId = GetRoleId(_PassportId);
            if (_RoleId != 1)
            {
                list = db.Queryable<AgentType>().InnerJoin<AgentDetails>((c, d) => c.Id == d.AgentId).Where((c, d) => d.PassportId == _PassportId).Select((c, d) => c).ToList();
            }

            if (!string.IsNullOrWhiteSpace(AgentTypeName))
            {
                list = (from c in list where c.AgentName.Contains(AgentTypeName) select c).ToList();
            }

            info.data_count = list.Count;
            if (PageIndex > 0)
            {
                list = (from c in list orderby c.AddTime descending select c).Skip((PageIndex - 1) * PageSize).Take(PageSize).ToList();
            }

            info.code = 200;
            info.msg = "success";
            info.data = list;
            return info;
        }


        /// <summary>
        /// 添加/修改代理类型(AgentId:""为添加，有值为修改)
        /// </summary>
        /// <param name="param">
        /// {"OpenId":"9e26f6e2244f7e34ed8b4eabdcb2f9c4","IsValid":"0禁用，1启用","AgentName":"代理名称","AgentDesc":"代理描述","AgentId":""}
        /// </param>
        /// <returns></returns>
        [HttpPost]
        public MsgInfo<string> PostAgent(dynamic param)
        {
            MsgInfo<string> info = new();
            DateTime t = DateTime.Now;
            var data = JsonConvert.DeserializeObject<dynamic>(param.ToString());
            string OpenId = data.OpenId.ToString();
            if (Object.ReferenceEquals(null, data))
            {
                info.code = 400;
                info.msg = "Network Exception";
                return info;
            }
            long _PassportId = IsLogin(OpenId);
            if (_PassportId <= 0)
            {
                info.code = 400;
                info.msg = "Please login";
                return info;
            }

            int _RoleId = GetRoleId(_PassportId);
            if (_RoleId != 1)
            {
                info.code = 400;
                info.msg = "no permission";
                return info;
            }

            string AgentName = data.AgentName.ToString();
            string AgentDesc = data.AgentDesc.ToString();
            string AgentId = data.AgentId.ToString();
            string IsValid = data.IsValid.ToString();
            int _IsValid = 1;
            int.TryParse(IsValid, out _IsValid);
            if (string.IsNullOrEmpty(AgentId))
            {
                AgentType agent = new()
                {
                    AddTime = t,
                    UpdateTime = t,
                    IsValid = _IsValid,
                    Sort = 1,
                    Ip = YRHelper.GetClientIPAddress(HttpContext),

                    AgentName = AgentName,
                    AgentDesc = AgentDesc,
                    PassportId = _PassportId
                };
                int m = db.Insertable(agent).ExecuteCommand();
                if (m < 1)
                {
                    info.code = 400;
                    info.msg = "Network Exception";
                    return info;
                }
            }
            else
            {
                int _AgentId = 0;
                int.TryParse(AgentId, out _AgentId);
                int m = db.Updateable<AgentType>().SetColumns(c => c.UpdateTime == t).
                    SetColumns(c => c.IsValid == _IsValid).
                    SetColumns(c => c.AgentName == AgentName).
                    SetColumns(c => c.AgentDesc == AgentDesc).
                    SetColumns(c => c.PassportId == _PassportId).
                    Where(c => c.Id == _AgentId).ExecuteCommand();
                if (m < 1)
                {
                    info.code = 400;
                    info.msg = "Network Exception";
                    return info;
                }
            }


            info.code = 200;
            info.msg = "success";

            return info;
        }


        /// <summary>
        /// 获取代理类型详情列表
        /// </summary>
        /// <param name="OpenId"></param>
        /// <param name="PageIndex"></param>
        /// <param name="PageSize"></param>
        /// <returns></returns>
        [HttpGet]
        public MsgInfo<List<RetuAgentDetails>> GetAgentDetails(string OpenId, int PageIndex, int PageSize = 20)
        {
            MsgInfo<List<RetuAgentDetails>> info = new();
            long _PassportId = IsLogin(OpenId);
            if (_PassportId <= 0)
            {
                info.code = 400;
                info.msg = "Please login";
                return info;
            }
            List<RetuAgentDetails> list = db.Queryable<AgentDetails>().
                LeftJoin<AgentType>((c, d) => c.AgentId == d.Id).
                Where(c => c.PassportId == _PassportId).
                Select((c, d) => new RetuAgentDetails()
                {
                    Id = c.Id,
                    AddTime = c.AddTime,
                    UpdateTime = c.UpdateTime,
                    IsValid = c.IsValid,
                    Sort = c.Sort,
                    Ip = c.Ip,

                    PassportId = c.PassportId,
                    AgentName = d.AgentName,
                    AgentDesc = d.AgentDesc,
                    //Create = db.Queryable<AdminUser>().First(m => m.PassportId == d.PassportId).UserName

                    AgentId = c.AgentId,
                    AgentLevel = c.AgentLevel,
                    AgentBetIsRebate = c.AgentBetIsRebate,
                    AgentBetRebateRatio = c.AgentBetRebateRatio,
                    AgentPopIsRebate = c.AgentPopIsRebate,
                    AgentPopAmount = c.AgentPopAmount,
                    AgentPopCount = c.AgentPopCount,
                    AgentCashMethod = c.AgentCashMethod,
                    AgentRebateFrozenTime = c.AgentRebateFrozenTime,
                    AgentPayMethod = c.AgentPayMethod,
                    AgentCashMinAmount = c.AgentCashMinAmount,
                    AgentCashFee = c.AgentCashFee

                }).
                ToList();
            info.data_count = list.Count;
            if (PageIndex > 0)
            {
                list = (from c in list orderby c.AddTime descending select c).Skip((PageIndex - 1) * PageSize).Take(PageSize).ToList();
            }

            info.code = 200;
            info.msg = "success";
            info.data = list;
            return info;
        }

        ///<summary>  
        /// 修改代理类型详情
        /// </summary>
        /// <param name="param">
        /// {"OpenId":"9e26f6e2244f7e34ed8b4eabdcb2f9c4","IsValid":"(0关闭，1开启)","AgentLevel":"代理层级","AgentBetIsRebate":"代理自投返佣(0关闭，1开启)",
        /// "AgentBetRebateRatio":"代理自投返佣比例","AgentPopIsRebate":"推广返佣","AgentPopAmount":"推广单价","AgentPopCount":"推广限额","AgentLevel":"代理层级",
        /// "AgentCashMethod":"提现方式(0手动，1自动)","AgentRebateFrozenTime":"提现冻结时间","AgentPayMethod":"提现支付方式",
        /// "AgentCashMinAmount":"提现最小金额","AgentCashFee":"提现手续费",
        /// "AgentDetailsId":""}
        /// </param>
        /// <returns></returns>
        [HttpPost]
        public MsgInfo<string> PostAgentDetails(dynamic param)
        {
            MsgInfo<string> info = new();
            DateTime t = DateTime.Now;
            var data = JsonConvert.DeserializeObject<dynamic>(param.ToString());
            string OpenId = data.OpenId.ToString();
            if (Object.ReferenceEquals(null, data))
            {
                info.code = 400;
                info.msg = "Network Exception";
                return info;
            }
            long _PassportId = IsLogin(OpenId);
            if (_PassportId <= 0)
            {
                info.code = 400;
                info.msg = "Please login";
                return info;
            }

            string GoogleCode = data.GoogleCode.ToString();
            var mys = db.Queryable<AdminUser>().First(d => d.OpenId == OpenId);
            if (mys.IsGoogle == 1)
                if (!GoogleHelp.CheckCode(GoogleCode, mys.GoogleKey))
                {
                    info.code = 400;
                    info.msg = "Verification code error";
                    return info;
                }


            string IsValid = data.IsValid.ToString();
            string AgentLevel = data.AgentLevel.ToString();
            string AgentBetIsRebate = data.AgentBetIsRebate.ToString();
            string AgentBetRebateRatio = data.AgentBetRebateRatio.ToString();
            string AgentPopIsRebate = data.AgentPopIsRebate.ToString();
            string AgentPopAmount = data.AgentPopAmount.ToString();
            string AgentPopCount = data.AgentPopCount.ToString();
            string AgentCashMethod = data.AgentCashMethod.ToString();
            string AgentRebateFrozenTime = data.AgentRebateFrozenTime.ToString();
            string AgentPayMethod = data.AgentPayMethod.ToString();
            string AgentCashMinAmount = data.AgentCashMinAmount.ToString();
            string AgentCashFee = data.AgentCashFee.ToString();
            string AgentDetailsId = data.AgentDetailsId.ToString();

            int _IsValid = 0;
            int.TryParse(IsValid, out _IsValid);
            int _AgentLevel = 0;
            int.TryParse(AgentLevel, out _AgentLevel);
            int _AgentBetIsRebate = 0;
            int.TryParse(AgentBetIsRebate, out _AgentBetIsRebate);
            decimal _AgentBetRebateRatio = 0;
            decimal.TryParse(AgentBetRebateRatio, out _AgentBetRebateRatio);
            int _AgentPopIsRebate = 0;
            int.TryParse(AgentPopIsRebate, out _AgentPopIsRebate);
            decimal _AgentPopAmount = 0;
            decimal.TryParse(AgentPopAmount, out _AgentPopAmount);
            int _AgentPopCount = 0;
            int.TryParse(AgentPopCount, out _AgentPopCount);
            int _AgentCashMethod = 0;
            int.TryParse(AgentCashMethod, out _AgentCashMethod);
            int _AgentRebateFrozenTime = 0;
            int.TryParse(AgentRebateFrozenTime, out _AgentRebateFrozenTime);
            int _AgentPayMethod = 0;
            int.TryParse(AgentPayMethod, out _AgentPayMethod);
            decimal _AgentCashMinAmount = 0;
            decimal.TryParse(AgentCashMinAmount, out _AgentCashMinAmount);
            decimal _AgentCashFee = 0;
            decimal.TryParse(AgentCashFee, out _AgentCashFee);
            int _AgentDetailsId = 0;
            int.TryParse(AgentDetailsId, out _AgentDetailsId);

            AgentDetails agentDetails = db.Queryable<AgentDetails>().First(c => c.PassportId == _PassportId && c.IsValid == 1 && c.Id != _AgentDetailsId);
            if (agentDetails != null && _IsValid == 1)
            {
                info.code = 400;
                info.msg = "There is already an open agent type, please close it first";
                return info;
            }
            if (_AgentLevel > 1000)
            {
                info.code = 400;
                info.msg = "Agent level cannot exceed 1000";
                return info;
            }

            int m = db.Updateable<AgentDetails>().
                SetColumns(c => c.IsValid == _IsValid).
                SetColumns(c => c.AgentLevel == _AgentLevel).
                SetColumns(c => c.AgentBetIsRebate == _AgentBetIsRebate).
                SetColumns(c => c.AgentBetRebateRatio == _AgentBetRebateRatio).
                SetColumns(c => c.AgentPopIsRebate == _AgentPopIsRebate).
                SetColumns(c => c.AgentPopAmount == _AgentPopAmount).
                SetColumns(c => c.AgentPopCount == _AgentPopCount).
                SetColumns(c => c.AgentCashMethod == _AgentCashMethod).
                SetColumns(c => c.AgentRebateFrozenTime == _AgentRebateFrozenTime).
                SetColumns(c => c.AgentPayMethod == _AgentPayMethod).
                SetColumns(c => c.AgentCashMinAmount == _AgentCashMinAmount).
                SetColumns(c => c.AgentCashFee == _AgentCashFee).
                Where(c => c.Id == _AgentDetailsId).ExecuteCommand();
            if (m < 1)
            {
                info.code = 400;
                info.msg = "Network Exception";
                return info;
            }

            #region 代理层级添加
            var selectRanks = db.Queryable<AgentRank>().Where(c => c.AgentDetailsId == _AgentDetailsId).ToList();
            if (selectRanks?.Count == 0)
            {
                List<AgentRank> ranks = new List<AgentRank>();
                for (int i = 0; i < _AgentLevel; i++)
                {
                    AgentRank agentRank = new()
                    {
                        AddTime = t,
                        UpdateTime = t,
                        IsValid = 1,
                        Sort = 1,
                        Ip = YRHelper.GetClientIPAddress(HttpContext),

                        AgentDetailsId = _AgentDetailsId,
                        AgentLevel = (i + 1),
                        PassportId = _PassportId
                    };

                    ranks.Add(agentRank);
                }
                int n = db.Insertable(ranks).ExecuteCommand();
                if (n < _AgentLevel)
                {
                    info.code = 400;
                    info.msg = "Network Exception";
                    return info;
                }
            }
            #endregion

            info.code = 200;
            info.msg = "success";

            return info;
        }

        /// <summary>
        /// 修改代理等级
        /// </summary>
        /// <param name="param">
        /// {"OpenId":"9e26f6e2244f7e34ed8b4eabdcb2f9c4",
        /// "RebateRatio1":"反水比例","RebateRatio2":"返水极差","AgentValidBet":"有效流水","MaxRebateAmount":"返水上限",
        /// "OtherBet":"额外流水","OtherRebateRatio":"额外返水","OtherRebateNumber":"额外人数",
        /// "AgentRankId":"代理等级ID"}
        /// </param>
        /// <returns></returns>
        [HttpPost]
        public MsgInfo<string> PostAgentLevel(dynamic param)
        {
            MsgInfo<string> info = new();
            DateTime t = DateTime.Now;
            var data = JsonConvert.DeserializeObject<dynamic>(param.ToString());
            string OpenId = data.OpenId.ToString();
            if (Object.ReferenceEquals(null, data))
            {
                info.code = 400;
                info.msg = "Network Exception";
                return info;
            }
            long _PassportId = IsLogin(OpenId);
            if (_PassportId <= 0)
            {
                info.code = 400;
                info.msg = "Please login";
                return info;
            }
            string GoogleCode = data.GoogleCode.ToString();
            var mys = db.Queryable<AdminUser>().First(d => d.OpenId == OpenId);
            if (mys.IsGoogle == 1)
                if (!GoogleHelp.CheckCode(GoogleCode, mys.GoogleKey))
                {
                    info.code = 400;
                    info.msg = "Verification code error";
                    return info;
                }
            string RebateRatio1 = data.RebateRatio1.ToString();
            string RebateRatio2 = data.RebateRatio2.ToString();
            string AgentValidBet = data.AgentValidBet.ToString();
            string MaxRebateAmount = data.MaxRebateAmount.ToString();
            string OtherBet = data.OtherBet.ToString();
            string OtherRebateRatio = data.OtherRebateRatio.ToString();
            string OtherRebateNumber = data.OtherRebateNumber.ToString();
            string AgentRankId = data.AgentRankId.ToString();

            decimal _RebateRatio1 = 0;
            decimal.TryParse(RebateRatio1, out _RebateRatio1);
            decimal _RebateRatio2 = 0;
            decimal.TryParse(RebateRatio2, out _RebateRatio2);
            decimal _AgentValidBet = 0;
            decimal.TryParse(AgentValidBet, out _AgentValidBet);
            decimal _MaxRebateAmount = 0;
            decimal.TryParse(MaxRebateAmount, out _MaxRebateAmount);
            decimal _OtherBet = 0;
            decimal.TryParse(OtherBet, out _OtherBet);
            decimal _OtherRebateRatio = 0;
            decimal.TryParse(OtherRebateRatio, out _OtherRebateRatio);
            int _OtherRebateNumber = 0;
            int.TryParse(OtherRebateNumber, out _OtherRebateNumber);
            //int _AgentRankId = 0;
            //int.TryParse(AgentRankId, out _AgentRankId);

            int[] IntAgentRankIds = Array.ConvertAll<string, int>(AgentRankId.Split(new char[] { ',' }, StringSplitOptions.RemoveEmptyEntries), s => int.Parse(s));

            decimal First_RebateRatio1 = _RebateRatio1;
            for (int i = 0; i < IntAgentRankIds.Length; i++)
            {
                var current_agent_rank_id = IntAgentRankIds[i];
                if (i > 0)
                {
                    First_RebateRatio1 = First_RebateRatio1 - (First_RebateRatio1 * _RebateRatio2 / 100);
                }

                List<SugarParameter> list = new List<SugarParameter>();
                list.Add(new SugarParameter("@UpdateTime", t));
                list.Add(new SugarParameter("@AgentRankId", current_agent_rank_id));

                string sql_set = " UpdateTime=@UpdateTime,";
                if (!string.IsNullOrWhiteSpace(RebateRatio1))
                {
                    sql_set += " RebateRatio1=@RebateRatio1, ";
                    list.Add(new SugarParameter("@RebateRatio1", First_RebateRatio1));
                }
                if (!string.IsNullOrWhiteSpace(RebateRatio2))
                {
                    sql_set += " RebateRatio2=@RebateRatio2, ";
                    list.Add(new SugarParameter("@RebateRatio2", _RebateRatio2));
                }
                if (!string.IsNullOrWhiteSpace(AgentValidBet))
                {
                    sql_set += " AgentValidBet=@AgentValidBet, ";
                    list.Add(new SugarParameter("@AgentValidBet", _AgentValidBet));
                }
                if (!string.IsNullOrWhiteSpace(MaxRebateAmount))
                {
                    sql_set += " MaxRebateAmount=@MaxRebateAmount, ";
                    list.Add(new SugarParameter("@MaxRebateAmount", _MaxRebateAmount));
                }
                if (!string.IsNullOrWhiteSpace(OtherBet))
                {
                    sql_set += " OtherBet=@OtherBet, ";
                    list.Add(new SugarParameter("@OtherBet", _OtherBet));
                }
                if (!string.IsNullOrWhiteSpace(OtherRebateRatio))
                {
                    sql_set += " OtherRebateRatio=@OtherRebateRatio, ";
                    list.Add(new SugarParameter("@OtherRebateRatio", _OtherRebateRatio));
                }
                if (!string.IsNullOrWhiteSpace(OtherRebateNumber))
                {
                    sql_set += " OtherRebateNumber=@OtherRebateNumber, ";
                    list.Add(new SugarParameter("@OtherRebateNumber", _OtherRebateNumber));
                }
                sql_set = sql_set.Trim().Trim(',');

                string sql_update = " update AgentRank set " + sql_set + " where id in (@AgentRankId)";
                int m = db.Ado.ExecuteCommand(sql_update, list);
                if (m < 1)
                {
                    LogHelper.Debug($"ActionName(PostAgentLevel):{sql_update}");
                    info.code = 400;
                    info.msg = "Network Exception";
                    return info;
                }
            }
            info.code = 200;
            info.msg = "success";
            return info;
        }

        /// <summary>
        /// 获取代理等级列表
        /// </summary>
        /// <param name="OpenId"></param>
        /// <param name="AgentDetailsId"></param>
        /// <param name="PageIndex"></param>
        /// <param name="PageSize"></param>
        /// <returns></returns>
        [HttpGet]
        public MsgInfo<List<AgentRank>> GetAgentLevel(string OpenId, string AgentDetailsId, int PageIndex, int PageSize = 20)
        {
            MsgInfo<List<AgentRank>> info = new();
            long _PassportId = IsLogin(OpenId);
            if (_PassportId <= 0)
            {
                info.code = 400;
                info.msg = "Please login";
                return info;
            }

            int _AgentDetailsId = 0;
            if (!string.IsNullOrWhiteSpace(AgentDetailsId))
            {
                int.TryParse(AgentDetailsId, out _AgentDetailsId);
            }

            List<AgentRank> list = db.Queryable<AgentRank>().Where(c => c.AgentDetailsId == _AgentDetailsId).ToList();
            info.data_count = list.Count;
            if (PageIndex > 0)
            {
                list = (from c in list orderby c.AddTime descending select c).Skip((PageIndex - 1) * PageSize).Take(PageSize).ToList();
            }

            info.code = 200;
            info.msg = "success";
            info.data = list;
            return info;
        }

        /// <summary>
        /// 获取用户详情
        /// </summary>
        /// <param name="OpenId"></param>
        /// <param name="PassportId">PassportId</param>
        /// <returns></returns>
        [HttpGet]
        public MsgInfo<RetuUserInfoDetails> GetMemberDetailsMsg(string OpenId, string PassportId)
        {
            MsgInfo<RetuUserInfoDetails> info = new();
            long _PassportId = IsLogin(OpenId);
            if (_PassportId <= 0)
            {
                info.code = 400;
                info.msg = "Please login";
                return info;
            }
            int UserPassportId = 0;
            int.TryParse(PassportId, out UserPassportId);

            UserInfo userInfo = db.Queryable<UserInfo>().First(c => c.PassportId == UserPassportId);
            var bet = db.Queryable<Bet>().Where(c => c.PassportId == UserPassportId);
            List<RetuEventDetails> eventDetails = db.Queryable<EventDetails>().InnerJoin<EventHK>((c, d) => c.Guid == d.GUID).
                Where(c => c.PassportId == userInfo.PassportId).
                Select((c, d) => new RetuEventDetails
                {
                    AddTime = c.AddTime,
                    UpdateTime = c.UpdateTime,
                    GameTypeId = d.GameTypeId,
                    EventName = d.EventName,
                    IsAudit = d.IsAudit,
                    BetAmount = c.BetAmount,
                    OperationTime = d.OperationTime
                }).
                ToList();
            eventDetails.ForEach(d => d.GameName = db.Queryable<GameType>().First(x => d.GameTypeId.Contains(x.Id.ToString())).GameName);

            var TGAccount = JsonConvert.DeserializeObject<RetuOtherMsg>(userInfo.OtherMsg)?.TGAccount;
            var WhatsApp = JsonConvert.DeserializeObject<RetuOtherMsg>(userInfo.OtherMsg)?.WhatsApp;
            var UserName = db.Queryable<UserInfo>().First(c => c.PassportId == userInfo.PassportId)?.UserName;
            var PopVaild = JsonConvert.DeserializeObject<RetuOtherMsg>(userInfo.OtherMsg)?.PopVaild;
            var Remarks = JsonConvert.DeserializeObject<RetuOtherMsg>(userInfo.OtherMsg)?.Remarks;
            UserBasic userBasic = new()
            {
                HashAddress = userInfo.HashAddress,
                Telegram = TGAccount,
                WhatsApp = WhatsApp,
                ParentId = UserName,
                Level = "",
                IsVaild = userInfo.IsValid.ToString(),
                PopVaild = PopVaild,
                AddTime = userInfo.AddTime,
                Remarks = Remarks
            };

            List<ReturnBet> bets = bet.Select(x => new ReturnBet()
            {
                AddTime = x.AddTime,
                BetCoin = x.BetCoin,
                BetWinCoin = x.BetWinCoin,
                BetOdds = x.BetOdds,
                BetResult = x.BetResult,
                BetTime = x.BetTime,
                GameDetailsId = x.GameDetailsId,
                //GameTypeName = db.Queryable<GameType>().First(c => c.Id == x.GameDetailsId).GameName,
                Id = x.Id,
                Ip = x.Ip,
                IsValid = x.IsValid,
                ManageUserPassportId = x.ManageUserPassportId,
                OrderID = x.OrderID,
                PassportId = x.PassportId,
                product_ref = x.product_ref,
                SettlementState = x.SettlementState,
                SettlementTime = x.SettlementTime,
                Sort = x.Sort,
                UpdateTime = x.UpdateTime
            }).ToList();
            bets.ForEach(x => x.GameTypeName = db.Queryable<GameType>().First(c => c.Id == x.GameDetailsId).GameName);

            decimal Recharge = db.Queryable<RechargeCashOrder>().Where(c => c.PassportId == UserPassportId && c.RCType == 0).Sum(c => c.CoinNumber);
            decimal Cash = db.Queryable<RechargeCashOrder>().Where(c => c.PassportId == UserPassportId && c.RCType == 1).Sum(c => c.CoinNumber);
            var Childe = GetMyChildsAllT<UserInfo>("UserInfo", "", userInfo.PassportId, "1");


            string sql = $@"
                CREATE TABLE #userinfo
                (
	                username nvarchar(20),
	                ParentId int,
	                MyLevel INT,
	                PassportId bigint
                );
                WITH T 
                AS (SELECT 
			                username,
			                ParentId,
							1 AS mylevel,
			                PassportId
	                FROM 
	                (SELECT username,ParentId,PassportId FROM UserInfo AS b where b.ParentId=@PassportId) AS c
	                UNION ALL
	                SELECT 
			                a.username,
			                a.ParentId,
			                b.mylevel + 1,
			                a.PassportId
	                FROM UserInfo a
		                JOIN T b
			                ON a.ParentId = b.PassportId and a.ParentId=b.ParentId 
			                )
			                INSERT INTO #userinfo
			                (
				                username,
								ParentId,
				                MyLevel,
				                PassportId
			                ) SELECT * FROM T   ORDER BY T.mylevel DESC  OPTION (MAXRECURSION 32700);
                            SELECT * FROM #userinfo
                DROP TABLE #userinfo 
            ";

            SugarParameter[] sugars = new SugarParameter[] {
                new SugarParameter("@PassportId",userInfo.PassportId)
            };
            List<MyChild> mies = db.Ado.SqlQuery<MyChild>(sql, sugars);//比db.SqlQueryable兼容性更强，支持复杂SQL存储过程，缺点没有自带的分页操作



            var userChilde = (from c in Childe
                              select new RetuUserInfo
                              {
                                  PassportId = c.PassportId,
                                  UserName = c.UserName,
                                  HashAddress = c.HashAddress,
                                  AgentTypeID = c.AgentTypeID,
                                  AddTime = c.AddTime,
                                  OtherMsg = !string.IsNullOrWhiteSpace(c.OtherMsg) ? JsonConvert.DeserializeObject<RetuOtherMsg>(c.OtherMsg) : null,
                                  MyLevel = mies.Count > 1 ? mies.Where(x => x.PassportId == c.PassportId).Select(x => x.MyLevel).FirstOrDefault() : 1
                              }).ToList();

            RetuUserInfoDetails userInfoDetails = new()
            {
                UID = PassportId,
                UserName = userInfo?.UserName,
                BetCount = bet.ToList()?.Count,
                BetAmount = bet.Sum(c => c.BetCoin),
                RebateAmount = db.Queryable<RebateDetails>().Where(c => c.PassportId == UserPassportId).Sum(c => c.RebateAmount),
                BetWinLose = Cash - Recharge,
                Discount = eventDetails.Sum(c => c.BetAmount),
                userBasic = userBasic,
                userBets = bets,
                userDiscount = eventDetails,
                userChilde = userChilde
            };

            info.code = 200;
            info.msg = "success";
            info.data = userInfoDetails;
            return info;
        }
        #endregion



        /// <summary>
        /// 获取商家哈希地址
        /// </summary>
        /// <param name="param">{"OpenId":"9e26f6e2244f7e34ed8b4eabdcb2f9c4","PassportId":"","CustomerId":"1"}</param>
        /// <returns></returns>
        [HttpPost]
        public PayRetu GetShopAddress(dynamic param)
        {
            #region 静态无错
            //PayRetu info = new();
            //DateTime t = DateTime.Now;
            //var data = JsonConvert.DeserializeObject<dynamic>(param.ToString());
            //if (Object.ReferenceEquals(null, data))
            //{
            //    info.code = 400;
            //    info.msg = "Network Exception";
            //    return info;
            //}
            //string OpenId = data.OpenId.ToString();
            //string CustomerId = data.CustomerId.ToString();
            //long _PassportId = IsLogin(OpenId);
            //if (_PassportId <= 0)
            //{
            //    info.code = 400;
            //    info.msg = "Please login";
            //    return info;
            //}

            //var merchant_no = "100004";
            //var params1 = "{\"extra\":{},\"customerid\":\"" + CustomerId + "\",\"currency\":\"USDT\"}";
            //var sign_type = "MD5";
            //var timestamp = convert_time_int_to10(DateTime.Now);
            //var apiKey = "e1ea605e071b38ca0bb38b1883876cb7";

            //string sign_key = merchant_no + params1 + sign_type + timestamp + apiKey;
            //string sign = YRHelper.get_md5_32(sign_key);

            ////post请求
            //{
            //    try
            //    {


            //        string str_json = "{merchant_no: \"100004\",params:\"{\\\"extra\\\":{},\\\"customerid\\\":\\\"" + CustomerId + "\\\",\\\"currency\\\":\\\"USDT\\\"}\",sign: \"" + sign + "\",sign_type: \"MD5\",timestamp: " + timestamp + " }";
            //        string json = JsonConvert.DeserializeObject<dynamic>(str_json).ToString();
            //        string result = HttpHelper.Helper.PostMoths("https://api.paypptp.com/api/gateway/bind-customer", json);
            //        JObject jo = JObject.Parse(result);
            //        string _params = jo["params"].Value<string>();
            //        int code = jo["code"].Value<int>();
            //        string message = jo["message"].Value<string>();
            //        int _timestamp = jo["timestamp"].Value<int>();
            //        if (code != 200)
            //        {
            //            info.code = 400;
            //            info.msg = "Network Exception";
            //            return info;
            //        }
            //        info = JsonConvert.DeserializeObject<PayRetu>(_params);
            //    }
            //    catch (Exception ex)
            //    {

            //          LogHelper.Error("GetShopAddress:" + ex.Message);
            //    }
            //}
            //info.code = 200;
            //info.msg = "success";

            //return info;
            #endregion


            //PayRetu info = new();
            //DateTime t = DateTime.Now;
            //var data = JsonConvert.DeserializeObject<dynamic>(param.ToString());
            //if (Object.ReferenceEquals(null, data))
            //{
            //    info.code = 400;
            //    info.msg = "Network Exception";
            //    return info;
            //}
            //string OpenId = data.OpenId.ToString();
            //string CustomerId = data.CustomerId.ToString();
            //string PassportId = data.PassportId.ToString();
            //long ShopPassportId = 0;
            //long.TryParse(PassportId, out ShopPassportId);
            //long _PassportId = IsLogin(OpenId);
            //if (_PassportId <= 0)
            //{
            //    info.code = 400;
            //    info.msg = "Please login";
            //    return info;
            //}

            //string _merchant_no = db.Queryable<PaySys>().First(c => c.PassportId == ShopPassportId)?.merchant_no;
            //if (string.IsNullOrWhiteSpace(_merchant_no))
            //{
            //    info.code = 400;
            //    info.msg = "Please configure payment first";
            //    return info;
            //}
            //PaySys pay = GetPayMsg(_merchant_no);
            //var merchant_no = pay?.merchant_no;
            //var params1 = "{\"extra\":{},\"customerid\":\"" + CustomerId + "\",\"currency\":\"USDT\"}";
            //var sign_type = "MD5";
            //var timestamp = convert_time_int_to10(DateTime.Now);
            //var apiKey = pay?.apiKey;

            //string sign_key = merchant_no + params1 + sign_type + timestamp + apiKey;
            //string sign = YRHelper.get_md5_32(sign_key);

            ////post请求
            //{
            //    try
            //    {
            //        string str_json = "{merchant_no: \"" + pay?.merchant_no + "\",params:\"{\\\"extra\\\":{},\\\"customerid\\\":\\\"" + CustomerId + "\\\",\\\"currency\\\":\\\"USDT\\\"}\",sign: \"" + sign + "\",sign_type: \"MD5\",timestamp: " + timestamp + " }";
            //        string json = JsonConvert.DeserializeObject<dynamic>(str_json).ToString();
            //        LogHelper.Debug($"ActionName(CrateBots),param:{json.Trim()}");
            //        string result = HttpHelper.Helper.PostMoths(pay.request_url3, json.Trim());
            //        JObject jo = JObject.Parse(result);
            //        string _params = jo["params"].Value<string>();
            //        int code = jo["code"].Value<int>();
            //        string message = jo["message"].Value<string>();
            //        int _timestamp = jo["timestamp"].Value<int>();
            //        if (code != 200)
            //        {
            //            info.code = 400;
            //            info.msg = "Network Exception";
            //            return info;
            //        }
            //        info = JsonConvert.DeserializeObject<PayRetu>(_params);
            //        LogHelper.Debug($"ActionName(GetShopAddress):{result}");
            //    }
            //    catch (Exception ex)
            //    {

            //        LogHelper.Error("GetShopAddress:" + ex.Message);
            //    }
            //}
            //info.code = 200;
            //info.msg = "success";

            //return info;



            PayRetu info = new();
            DateTime t = DateTime.Now;
            var data = JsonConvert.DeserializeObject<dynamic>(param.ToString());
            if (Object.ReferenceEquals(null, data))
            {
                info.code = 400;
                info.msg = "Network Exception";
                return info;
            }
            string OpenId = data.OpenId.ToString();
            string CustomerId = data.CustomerId.ToString();
            string PassportId = data.PassportId.ToString();
            long ShopPassportId = 0;
            long.TryParse(PassportId, out ShopPassportId);
            long _PassportId = IsLogin(OpenId);
            if (_PassportId <= 0)
            {
                info.code = 400;
                info.msg = "Please login";
                return info;
            }

            string _merchant_no = db.Queryable<PaySys>().First(c => c.PassportId == ShopPassportId)?.merchant_no;
            if (string.IsNullOrWhiteSpace(_merchant_no))
            {
                info.code = 400;
                info.msg = "Please configure payment first";
                return info;
            }
            PaySys pay = GetPayMsg(_merchant_no);
            var apikey1 = pay?.apiKey;
            var timestamp1 = convert_time_int_to10(DateTime.Now);
            paramsAddress paramss1 = new paramsAddress() { customerid = CustomerId, currency = "USDT" };
            var json1 = JsonConvert.SerializeObject(paramss1);
            Dictionary<string, string> valuePairs = new Dictionary<string, string>();
            valuePairs.Add("merchant_no", _merchant_no);
            valuePairs.Add("timestamp", timestamp1.ToString());
            valuePairs.Add("sign_type", "MD5");
            valuePairs.Add("params", json1);
            //valuePairs.Add("params", shopAddress.paramss);
            var sg1 = _merchant_no + json1 + "MD5" + timestamp1 + apikey1;
            valuePairs.Add("sign", YRHelper.get_md5_32(sg1));

            //post请求
            {
                try
                {
                    string result = HttpHelper.Helper.PostMothss(pay?.request_url3, valuePairs);
                    //string result = HttpHelper.Helper.PostMoths(pay?.request_url3, JsonConvert.SerializeObject(shopAddress));
                    JObject jo = JObject.Parse(result);
                    string _params = jo["params"].Value<string>();
                    int code = jo["code"].Value<int>();
                    string message = jo["message"].Value<string>();
                    int _timestamp = jo["timestamp"].Value<int>();
                    if (code != 200)
                    {
                        info.code = 400;
                        info.msg = "Network Exception";
                        return info;
                    }
                    info = JsonConvert.DeserializeObject<PayRetu>(_params);
                    LogHelper.Debug($"ActionName(GetShopAddress):{result}");
                }
                catch (Exception ex)
                {
                    LogHelper.Error("GetShopAddress:" + ex.Message);
                }
            }
            info.code = 200;
            info.msg = "success";

            return info;

        }

        /// <summary>
        /// 获取用户收款地址与游戏关联列表
        /// </summary>
        /// <param name="OpenId"></param>
        /// <param name="ManageId">管理员ID</param>
        /// <returns></returns>
        [HttpGet]
        public MsgInfo<List<RetuAdminGameRelation>> GetAdminGameRelationList(string OpenId, int ManageId)
        {
            MsgInfo<List<RetuAdminGameRelation>> info = new();
            DateTime t = DateTime.Now;
            long PassportId = IsLogin(OpenId);
            if (PassportId <= 0)
            {
                info.code = 400;
                info.msg = "Please login";
                return info;
            }

            long _PassportId = 0;
            AdminUser admin = db.Queryable<AdminUser>().First(c => c.Id == ManageId);
            if (admin == null)
            {
                info.code = 400;
                info.msg = "Network Exception";
                return info;
            }
            _PassportId = admin.PassportId;
            List<RetuAdminGameRelation> list = db.Queryable<AdminGameRelation>().LeftJoin<GameDetails>((c, d) => c.GameDetailsId == d.Id).
                Where(c => c.PassportId == _PassportId).
                Select((c, d) => new RetuAdminGameRelation
                {
                    Id = c.Id,
                    AddTime = c.AddTime,
                    UpdateTime = c.UpdateTime,
                    IsValid = c.IsValid,
                    Sort = c.Sort,
                    Ip = c.Ip,

                    PassportId = c.PassportId,
                    GameDetailsId = c.GameDetailsId,
                    GameDetailsName = d.GameName,
                    HashAddress = c.HashAddress,
                    CoinType = c.CoinType
                }).
                ToList();
            info.data_count = list.Count;

            info.code = 200;
            info.msg = "success";
            info.data = list;

            return info;
        }

        /// <summary>
        /// 更新用户收款地址与游戏关联  CoinType(0usdt 1其他)
        /// </summary>
        /// <param name="param">
        /// {"OpenId":"9e26f6e2244f7e34ed8b4eabdcb2f9c4","GameDetailsId":4,"HashAddress":"","CoinType":0}
        /// </param>
        /// <returns></returns>
        [HttpPost]
        public PayRetu PostAdminGameRelation(dynamic param)
        {
            PayRetu info = new();
            DateTime t = DateTime.Now;
            var data = JsonConvert.DeserializeObject<dynamic>(param.ToString());
            if (Object.ReferenceEquals(null, data))
            {
                info.code = 400;
                info.msg = "Network Exception";
                return info;
            }
            //string OpenId, int GameDetailsId, string HashAddress, int CoinType = 0
            string OpenId = data.OpenId.ToString();
            int GameDetailsId = 0;
            int.TryParse(data.GameDetailsId.ToString(), out GameDetailsId);
            string HashAddress = data.HashAddress.ToString();
            int CoinType = 0;
            int.TryParse(data.CoinType.ToString(), out CoinType);
            long _PassportId = IsLogin(OpenId);
            if (_PassportId <= 0)
            {
                info.code = 400;
                info.msg = "Please login";
                return info;
            }

            int i = db.Updateable<AdminGameRelation>().SetColumns(c => new AdminGameRelation()
            {
                UpdateTime = t,

                HashAddress = HashAddress,
                CoinType = CoinType
            }).Where(c => c.PassportId == _PassportId && c.GameDetailsId == GameDetailsId).ExecuteCommand();
            SugarParameter[] sugars = new SugarParameter[] {
                    new SugarParameter("@UpdateTime",t),
                    new SugarParameter("@Id",GameDetailsId),
                    new SugarParameter("@HashAddress",HashAddress),
                    new SugarParameter("@CoinType",CoinType),
                    new SugarParameter("@PassportId",_PassportId)
            };
            int m = db.Ado.ExecuteCommand(@"update AdminGameRelation set UpdateTime = @UpdateTime,HashAddress=@HashAddress,CoinType=@CoinType where Id = @Id", sugars);
            if (m < 1)
            {
                info.code = 400;
                info.msg = "Network Exception";
                return info;
            }

            info.code = 200;
            info.msg = "success";

            return info;
        }

        /// <summary>
        /// 添加异常出金订单
        /// </summary>
        /// 
        /// <returns></returns>
        private MsgInfo<string> PostCashErrorOrder(string OrderID, decimal Amount, string ToHashAddress, string _merchant_no, string product, string cash_json)
        {
            MsgInfo<string> info = new();
            DateTime t = DateTime.Now;

            CashErrorOrder cashError = db.Queryable<CashErrorOrder>().First(c => c.OrderID == OrderID);
            if (cashError == null)
            {
                CashErrorOrder od = new CashErrorOrder()
                {
                    AddTime = t,
                    UpdateTime = t,
                    IsValid = 1,
                    Sort = 1,
                    Ip = YRHelper.GetClientIPAddress(HttpContext),
                    
                    OrderID = OrderID,
                    Amount = Amount,
                    ToHashAddress = ToHashAddress,
                    merchant_no = _merchant_no,
                    cash_json = cash_json,
                    product = product??""
                };
                int m = db.Insertable(od).ExecuteCommand();
                if (m < 1)
                {
                    info.code = 400;
                    info.msg = "Network Exception";
                    return info;
                }
            }

            info.code = 200;
            info.msg = "success";
            return info;
        }

        /// <summary>
        /// 出金接口
        /// </summary>
        /// <param name="OrderID">订单ID</param>
        /// <param name="Amount"></param>
        /// <param name="ToHashAddress"></param>
        /// <param name="_merchant_no">商户号</param>
        /// <param name="product">产品类型</param>
        /// <returns></returns>
        private PayRetu PostWithdraw(string OrderID, decimal Amount, string ToHashAddress, string _merchant_no, string product)
        {
            //PayRetu info = new();
            //DateTime t = DateTime.Now;

            //PaySys pay = GetPayMsg(_merchant_no);
            //var merchant_no = pay?.merchant_no;
            ////var params1 = "{\"merchant_ref\":\"" + OrderID + "\",\"product\":\"USDT-TRC20Payout\",\"amount\":\"" + Amount + "\",\"extra\":{\"address\":\"" + ToHashAddress + "\"}}";
            //var params1 = "{\"merchant_ref\":\"" + OrderID + "\",\"product\":\"" + product + "\",\"amount\":\"" + Amount + "\",\"extra\":{\"address\":\"" + ToHashAddress + "\"}}";
            //var sign_type = "MD5";
            //var timestamp = convert_time_int_to10(DateTime.Now);
            //var apiKey = pay?.apiKey;
            //string str_json = string.Empty;

            //string sign_key = merchant_no + params1 + sign_type + timestamp + apiKey;
            //string sign = YRHelper.get_md5_32(sign_key);

            ////post请求
            //{
            //    try
            //    {
            //        //str_json = "{merchant_no: \"" + pay?.merchant_no + "\",params:\"{\\\"merchant_ref\\\":\\\"" + OrderID + "\\\",\\\"product\\\":\\\"USDT-TRC20Payout\\\",\\\"amount\\\":\\\"" + Amount + "\\\",\\\"extra\\\":{\\\"address\\\":\\\"" + ToHashAddress + "\\\"}}\",sign: \"" + sign + "\",sign_type: \"MD5\",timestamp: " + timestamp + " }";
            //        str_json = "{merchant_no: \"" + pay?.merchant_no + "\",params:\"{\\\"merchant_ref\\\":\\\"" + OrderID + "\\\",\\\"product\\\":\\\"" + product + "\\\",\\\"amount\\\":\\\"" + Amount + "\\\",\\\"extra\\\":{\\\"address\\\":\\\"" + ToHashAddress + "\\\"}}\",sign: \"" + sign + "\",sign_type: \"MD5\",timestamp: " + timestamp + " }";
            //        string json = JsonConvert.DeserializeObject<dynamic>(str_json).ToString();
            //        LogHelper.Debug($"出金接口(PostCash):{json}");
            //        string result = HttpHelper.Helper.PostMoths(pay.request_url2, json);
            //        JObject jo = JObject.Parse(result);
            //        string _params = jo["params"].Value<string>();
            //        int code = jo["code"].Value<int>();
            //        string message = jo["message"].Value<string>();
            //        int _timestamp = jo["timestamp"].Value<int>();
            //        if (code != 200)
            //        {
            //            info.code = 400;
            //            info.msg = "Network Exception";
            //            return info;
            //        }
            //        CashRetu cash = JsonConvert.DeserializeObject<CashRetu>(_params);
            //        info.Cash = cash;
            //    }
            //    catch (Exception ex)
            //    {
            //        LogHelper.Debug($"PostWithdraw:{ex.Message}   parmas|OrderID:{OrderID}|Amount:{Amount}|ToHashAddress:{ToHashAddress}|merchant_no:{merchant_no}");

            //        PostCashErrorOrder(OrderID, Amount, ToHashAddress, merchant_no, product, str_json);

            //        info.code = 500;
            //        info.msg = ex.Message;
            //        return info;
            //    }
            //}
            //info.code = 200;
            //info.msg = "success";

            //return info;




            PayRetu info = new();
            DateTime t = DateTime.Now;
            PaySys pay = GetPayMsg(_merchant_no);
            //post请求
            {
                try
                {
                    PayCash rtnPay = new PayCash() { merchant_no = pay?.merchant_no, timestamp = convert_time_int_to10(DateTime.Now), sign_type = "MD5" };
                    paramsCash paramss = new paramsCash()
                    {
                        extend_params = "",
                        product = product,
                        amount = Amount.ToString(),
                        merchant_ref = OrderID,
                        extra = new extras()
                        {
                            address = ToHashAddress
                        }
                    };
                    var json = JsonConvert.SerializeObject(paramss);
                    rtnPay.paramss = paramss;
                    Dictionary<string, string> valuePairs = new Dictionary<string, string>();
                    valuePairs.Add(nameof(rtnPay.merchant_no), rtnPay.merchant_no);
                    valuePairs.Add(nameof(rtnPay.timestamp), rtnPay.timestamp.ToString());
                    valuePairs.Add(nameof(rtnPay.sign_type), rtnPay.sign_type);
                    valuePairs.Add("params", json);
                    var sg = rtnPay.merchant_no + json + rtnPay.sign_type + rtnPay.timestamp + pay?.apiKey;
                    valuePairs.Add(nameof(rtnPay.sign), YRHelper.get_md5_32(sg));

                    rtnPay.sign = YRHelper.get_md5_32(sg);

                    string result = HttpHelper.Helper.PostMothss(pay?.request_url2, valuePairs);
                    JObject jo = JObject.Parse(result);
                    string _params = jo["params"].Value<string>();
                    int code = jo["code"].Value<int>();
                    string message = jo["message"].Value<string>();
                    int _timestamp = jo["timestamp"].Value<int>();
                    if (code != 200)
                    {
                        info.code = 400;
                        info.msg = "Network Exception";
                        return info;
                    }
                    CashRetu cash = JsonConvert.DeserializeObject<CashRetu>(_params);
                    info.Cash = cash;
                }
                catch (Exception ex)
                {
                    LogHelper.Debug($"PostWithdraw:{ex.Message}   parmas|OrderID:{OrderID}|Amount:{Amount}|ToHashAddress:{ToHashAddress}|merchant_no:{pay?.merchant_no}");

                    PostCashErrorOrder(OrderID, Amount, ToHashAddress, pay?.merchant_no, product, "");

                    info.code = 500;
                    info.msg = ex.Message;
                    return info;
                }
            }
            info.code = 200;
            info.msg = "success";

            return info;

        }


        /// <summary>
        /// 任务调度出金接口
        /// </summary>
        /// <param name="OrderID"></param>
        /// <param name="Amount"></param>
        /// <param name="ToHashAddress"></param>
        /// <param name="_merchant_no"></param>
        /// <returns></returns>
        private MsgInfo<string> PostCashAgin()
        {
            MsgInfo<string> info = new();
            List<CashErrorOrder> cashErrorOrders = db.Queryable<CashErrorOrder>().Where(c => c.IsValid == 1).ToList();
            for (int i = 0; i < cashErrorOrders.Count; i++)
            {
                string OrderID = cashErrorOrders[i].OrderID;
                decimal Amount = cashErrorOrders[i].Amount;
                string ToHashAddress = cashErrorOrders[i].ToHashAddress;
                string merchant_no = cashErrorOrders[i].merchant_no;
                string product = cashErrorOrders[i].product;
                PayRetu payRetu = PostWithdraw(OrderID, Amount, ToHashAddress, merchant_no, product);
                if (payRetu.code != 200)
                {
                    LogHelper.Debug($"PostCashAgin:{payRetu.msg}   parmas|OrderID:{OrderID}|Amount:{Amount}|ToHashAddress:{ToHashAddress}|merchant_no:{merchant_no}");
                    info.code = 400;
                    info.msg = "fail";
                    return info;
                }
            }
            info.code = 200;
            info.msg = "success";
            return info;
        }

        /// <summary>
        /// 出金接口
        /// </summary>
        /// <param name="OrderID">订单ID</param>
        /// <param name="Amount"></param>
        /// <param name="ToHashAddress"></param>
        /// <param name="ManagePassportId"></param>
        /// <param name="block_hash">哈希值</param>
        /// <param name="_merchant_no">商户号</param>
        /// <param name="product">产品类型</param>
        /// <param name="system_ref">支付系统编号</param>
        /// <param name="_RCType">充值提现(0充值，1提款，2优惠红利，3反水派发，4代理佣金，5代理手续费)</param>
        /// <returns></returns>
        [HttpPost]
        private PayRetu PostCash(string OrderID, decimal Amount, string ToHashAddress, Int64 ManagePassportId, string block_hash, string _merchant_no, string product = "", string system_ref = "",int _RCType=1)
        {
            #region 静态无错
            //PayRetu info = new();
            //DateTime t = DateTime.Now;

            //UserInfo userInfo = db.Queryable<UserInfo>().First(c => c.HashAddress == ToHashAddress);
            //if (userInfo == null)
            //{
            //    info.code = 400;
            //    info.msg = "Network Exception";
            //    return info;
            //}

            //RechargeCashOrder od = new RechargeCashOrder()
            //{
            //    AddTime = t,
            //    UpdateTime = t,
            //    IsValid = 1,
            //    Sort = 1,
            //    Ip = YRHelper.GetClientIPAddress(HttpContext),

            //    PassportId = userInfo.PassportId,
            //    OrderID = OrderID,
            //    FromHashAddress = "",
            //    ToHashAddress = ToHashAddress,
            //    CoinNumber = Amount,
            //    IsPayment = 0,//是否支付(0未支付，1已支付)
            //    CoinType = 0,//币类别(0usdt 1其他)
            //    RCType = 1,//充值提现(0充值 1提现)
            //    ManageUserPassportId = ManagePassportId,
            //    block_hash = block_hash
            //};
            //int m = db.Insertable(od).ExecuteCommand();
            //if (m < 1)
            //{
            //    info.code = 400;
            //    info.msg = "Network Exception";
            //    return info;
            //}

            //PaySys pay = GetPayMsg(FormHashAddress);

            ////var merchant_no = "100004";
            ////var params1 = "{\"merchant_ref\":\"" + OrderID + "\",\"product\":\"USDT-TRC20Payout\",\"amount\":\"" + Amount + "\",\"extra\":{\"address\":\"" + ToHashAddress + "\"}}";
            ////var sign_type = "MD5";
            ////var timestamp = convert_time_int_to10(DateTime.Now);
            ////var apiKey = "e1ea605e071b38ca0bb38b1883876cb7";

            //string sign_key = merchant_no + params1 + sign_type + timestamp + apiKey;
            //string sign = YRHelper.get_md5_32(sign_key);

            ////post请求
            //{
            //string str_json = "{merchant_no: \"100004\",params:\"{\\\"merchant_ref\\\":\\\"" + OrderID + "\\\",\\\"product\\\":\\\"USDT-TRC20Payout\\\",\\\"amount\\\":\\\"" + Amount + "\\\",\\\"extra\\\":{\\\"address\\\":\\\"" + ToHashAddress + "\\\"}}\",sign: \"" + sign + "\",sign_type: \"MD5\",timestamp: " + timestamp + " }";

            //    string json = JsonConvert.DeserializeObject<dynamic>(str_json).ToString();
            //    string result = HttpHelper.Helper.PostMoths("https://api.paypptp.com/api/gateway/withdraw", json);
            //    JObject jo = JObject.Parse(result);
            //    string _params = jo["params"].Value<string>();
            //    int code = jo["code"].Value<int>();
            //    string message = jo["message"].Value<string>();
            //    int _timestamp = jo["timestamp"].Value<int>();
            //    if (code != 200)
            //    {
            //        info.code = 400;
            //        info.msg = "Network Exception";
            //        return info;
            //    }
            //    CashRetu cash = JsonConvert.DeserializeObject<CashRetu>(_params);
            //    info.Cash = cash;
            //}
            //info.code = 200;
            //info.msg = "success";

            //return info;
            #endregion

            PayRetu info = new();
            DateTime t = DateTime.Now;

            UserInfo userInfo = db.Queryable<UserInfo>().First(c => c.HashAddress == ToHashAddress && c.ManageUserPassportId==ManagePassportId);
            if (userInfo == null)
            {
                info.code = 400;
                info.msg = "Network Exception";
                return info;
            }

            var CoinTypeId = db.Queryable<CoinType>().First(c => c.CoinValue == product).Id;
            RechargeCashOrder od = new RechargeCashOrder()
            {
                AddTime = t,
                UpdateTime = t,
                IsValid = 1,
                Sort = 1,
                Ip = YRHelper.GetClientIPAddress(HttpContext),

                PassportId = userInfo.PassportId,
                OrderID = OrderID,
                FromHashAddress = "",
                ToHashAddress = ToHashAddress,
                CoinNumber = Amount,
                IsPayment = 0,//是否支付(0未支付，1已支付)
                CoinType = CoinTypeId,//币类别(1usdt 2trx)
                RCType = _RCType,//充值提现(0充值，1提款，2优惠红利，3反水派发，4代理佣金，5代理手续费)
                ManageUserPassportId = ManagePassportId,
                block_hash = block_hash,
                block_number = system_ref//存储支付系统编号
            };
            int m = db.Insertable(od).ExecuteCommand();
            if (m < 1)
            {
                info.code = 400;
                info.msg = "Network Exception";
                return info;
            }

            info = PostWithdraw(OrderID, Amount, ToHashAddress, _merchant_no, product);
            if (info.code != 200)
            {
                info.code = 400;
                info.msg = "Network Exception";
                return info;
            }

            info.code = 200;
            info.msg = "success";

            return info;

        }
        [HttpPost]


        /// <summary>
        /// 投注回调
        /// </summary>
        /// <param name="merchant_ref"></param>
        /// <param name="product"></param>
        /// <returns></returns>
        [Route("/api/Game/GameBetCall")]
        [HttpPost]
        public async Task<string> GameBetCall(string merchant_ref, string product)
        {
            string retu = "";
            MsgInfo<string> info = new();
            DateTime t = DateTime.Now;
            string OrderId = "";
            string BetOrderId = "";
            string PayOrderId = "";
            string CashOrderId = "";
            string hash_no = "";

            string merchant_no = Request.Form["merchant_no"];

            string paramss = Request.Form["params"];

            string sign = Request.Form["sign"];

            string sign_type = Request.Form["sign_type"];

            string timestamp = Request.Form["timestamp"];

            Notify noti = JsonConvert.DeserializeObject<Notify>(paramss);

            //string apikey = "e1ea605e071b38ca0bb38b1883876cb7";
            string apikey = $"merchant_no{merchant_no}";
            if (redis.Exists($"merchant_no{merchant_no}"))
            {
                if (redis.Get($"merchant_no{merchant_no}") == null)
                {
                    string _apikey = db.Queryable<PaySys>().First(c => c.merchant_no == merchant_no)?.apiKey;

                    redis.Set($"merchant_no{merchant_no}", _apikey, 60 * 60 * 24);
                }
            }
            else
            {
                string _apikey = db.Queryable<PaySys>().First(c => c.merchant_no == merchant_no)?.apiKey;

                redis.Set($"merchant_no{merchant_no}", _apikey, 60 * 60 * 24);
            }
            apikey = redis.Get($"merchant_no{merchant_no}");

            string ssssign = merchant_no + paramss + sign_type + timestamp + apikey;

            string md5ssssign = YRHelper.get_md5_32(ssssign);

            if (redis.Exists(merchant_no))
            {
                return "Fail";
            }
            redis.Set(sign, sign, 5);

            if (md5ssssign == sign)
            {
                //1.获取回调信息，根据付款地址自动注册会员,根据付款产品类型获取币种类别
                //2.赋值收款地址，付款地址，付款金额，付款时间，付款状态
                //3.根据收款地址获取游戏类型id
                //4.根据游戏id获取对应的限额以及赔率、中奖状态
                //5.插入投注记录、充值记录
                //6.中奖调用出金接口
                decimal BetCoin = 0;
                Int64 _PassportId = 0;
                Int64 _UserName = 0;
                int GameDetailsId = 0;
                string FromAddress = "";
                string ToAddress = "";
                int PayStatus = 0;
                string PayTime = string.Empty;
                bool IsWinFlag = false;
                string OpenResult = string.Empty;
                decimal Odds = 0;
                decimal Odds0 = 0;
                decimal Odds1 = 0;
                decimal Odds2 = 0;
                decimal Odds3 = 0;
                decimal Odds4 = 0;
                decimal Odds5 = 0;
                decimal Odds6 = 0;
                decimal Odds7 = 0;
                decimal Odds8 = 0;
                decimal Odds9 = 0;
                decimal GameFee = 0;//抽水比例
                decimal InvalidGameFee = 0; //无效比例
                decimal BetWinCoin = 0;
                string GameDetailsName = string.Empty;
                int _CoinType = 0;
                string payproduct = string.Empty;//付款币种类型
                string CoinName = string.Empty;//付款币种类型名称
                Int64 ManagePassportId = 502423741;

                Notify notify = JsonConvert.DeserializeObject<Notify>(paramss);
                FromAddress = notify.from;
                ToAddress = notify.to;
                decimal.TryParse(notify.pay_amount.ToString().Trim(), out BetCoin);
                PayStatus = notify.status;
                PayTime = notify.success_time;
                hash_no = notify.block_hash;
                OrderId = t.Year.ToString() + t.Month.ToString() + t.Day.ToString() + t.Hour.ToString() + t.Minute.ToString() + t.Second.ToString() + t.Millisecond.ToString();
                BetOrderId = "BET" + OrderId;
                PayOrderId = "R" + OrderId;
                CashOrderId = "C" + OrderId;

                #region 解绑TG账户
                if (redis.Exists(FromAddress))
                {
                    if (redis.Get(FromAddress) != null)
                    {
                        string UnBindAddressJson = redis.Get(FromAddress).ToString();
                        UnBindAddress unBind = JsonConvert.DeserializeObject<UnBindAddress>(UnBindAddressJson);
                        if (notify.pay_amount.Trim() == unBind.Price.Trim())
                        {
                            //更新hash收款地址
                            List<SugarParameter> list_params = new List<SugarParameter>();
                            list_params.Add(new SugarParameter("@updatetime", t));
                            list_params.Add(new SugarParameter("@HashAddress1", unBind.HassAddress));
                            list_params.Add(new SugarParameter("@HashAddress2", FromAddress));
                            string sql_update = string.Format(@"update UserInfo set updatetime=@updatetime,HashAddress=@HashAddress1 where HashAddress=@HashAddress2");
                            int m = db.Ado.ExecuteCommand(sql_update, list_params);
                            if (m < 1)
                            {
                                return "";
                            }
                        }
                        redis.Del(FromAddress);
                    }
                    return "SUCCESS";
                }
                #endregion

                //根据收款地址获取游戏类型id
                {
                    AdminGameRelation gameRelation = db.Queryable<AdminGameRelation>().First(c => c.HashAddress == ToAddress);
                    GameDetailsId = gameRelation.GameDetailsId;
                    ManagePassportId = gameRelation.PassportId;
                }

                //根据付款产品类型获取币种类别
                {
                    CoinType coin = db.Queryable<CoinType>().First(c => c.CoinKey == notify.product);
                    _CoinType = coin.Id;
                    payproduct = coin.CoinValue;
                    CoinName = coin.CoinName;
                }

                Task<string> t2 = Task.Run(() =>
                {
                    //根据付款地址自动注册会员
                    {
                        UserInfo userInfo = db.Queryable<UserInfo>().First(c => c.HashAddress == FromAddress && c.ManageUserPassportId==ManagePassportId);
                        if (userInfo == null)
                        {
                            string UserName = string.Empty;
                            while (true)
                            {
                                UserName = YRHelper.GetRandom(8);
                                var user = db.Queryable<UserInfo>().First(c => c.UserName == UserName);
                                if (user == null)
                                {
                                    break;
                                }
                            }
                            dynamic dc = "{\"UserName\":\"" + UserName + "\",\"TgChatId\":\"\",\"HashAddress\":\"" + FromAddress + "\",\"Pwd\":\"123456\",\"ParentTJCode\":\"\",\"ManageUserPassportId\":" + ManagePassportId + ",\"OtherMsg\":{\"MemberSource\":\"5\"}}";
                            LogHelper.Debug($"ActionName(GameBetCall):{dc}");
                            MsgInfo<UserInfo> result = Register(dc);
                            if (result.code != 200)
                            {
                                return "FAIL";
                            }
                            _PassportId = (result.data as UserInfo).PassportId;
                            long.TryParse((result.data as UserInfo).UserName, out _UserName);
                        }
                        else
                        {
                            _PassportId = userInfo.PassportId;
                            long.TryParse(userInfo.UserName, out _UserName);
                        }
                    }
                    return retu;
                });

                Task<string> t3 = Task.Run(() =>
                {
                    //根据游戏id获取对应的限额以及赔率、中奖状态
                    {
                        decimal MinQuota = 0;
                        decimal MaxQuota = 0;
                        LogHelper.Debug($"GameBetCall:{GameDetailsName}|{ManagePassportId}|{GameDetailsId}");
                        GameDetails details = db.Queryable<GameDetails>().First(c => c.GameTypeId == GameDetailsId && c.ManageUserPassportId==ManagePassportId);
                        if (details == null)
                        {
                            return "FAIL";
                        }
                        int CurrentGameDetailsId = details.Id;
                        if (_CoinType==1)//1usdt,2trx
                        {
                            MinQuota = details.MinQuota;
                            MaxQuota = details.MaxQuota;
                        }
                        else
                        {
                            MinQuota = details.MinQuota1;
                            MaxQuota = details.MaxQuota1;
                        }
                        Odds = details.Odds;
                        Odds0 = details.Odds0;
                        Odds1 = details.Odds1;
                        Odds2 = details.Odds2;
                        Odds3 = details.Odds3;
                        Odds4 = details.Odds4;
                        Odds5 = details.Odds5;
                        Odds6 = details.Odds6;
                        Odds7 = details.Odds7;
                        Odds8 = details.Odds8;
                        Odds9 = details.Odds9;
                        GameFee = details.GameFee;
                        InvalidGameFee = details.InvalidGameFee;
                        GameDetailsName = details.GameName;
                        if (string.IsNullOrWhiteSpace(hash_no))
                        {
                            return "FAIL";
                        }
                        IsWinFlag = IsWin(GameDetailsId, GameFee, InvalidGameFee, MinQuota, MaxQuota, BetCoin, hash_no, Odds,
                            Odds0, Odds1, Odds2, Odds3, Odds4, Odds5,
                            Odds6, Odds7, Odds8, Odds9, out BetWinCoin, out OpenResult);
                    }
                    return retu;
                });
                await t2;
                await t3;
                if (t2.Result == "FAIL" || t3.Result == "FAIL")
                {
                    return "";
                }

                Task<string> t4 = Task.Run(() =>
                {
                    //插入投注记录
                    Bet betSelect = db.Queryable<Bet>().First(c => c.product_ref == notify.product_ref);
                    if (betSelect == null)
                    {
                        Bet bet = new()
                        {
                            AddTime = t,
                            UpdateTime = t,
                            IsValid = 1,
                            Sort = 1,
                            Ip = YRHelper.GetClientIPAddress(HttpContext),

                            PassportId = _PassportId,
                            OrderID = BetOrderId,
                            GameDetailsId = GameDetailsId,
                            BetTime = t,
                            BetCoin = BetCoin,
                            CoinType = _CoinType,
                            BetOdds = Odds,
                            BetWinCoin = BetWinCoin,
                            SettlementState = IsWinFlag ? 0 : 1,//结算状态(0未结算 1已结算)
                            BetResult = IsWinFlag ? 1 : 0,//投注结果(0输 1赢 2未开奖)
                            OpenResult = OpenResult,
                            product_ref = notify.product_ref,
                            ManageUserPassportId = ManagePassportId
                        };
                        int i = db.Insertable(bet).ExecuteReturnIdentity();
                        if (i < 1)
                        {
                            return "FAIL";
                        }

                        #region 返佣
                        if (GameDetailsId == 5)//三公
                        {
                            if (OpenResult!="和")
                            {
                                GetMyParentRebate("1", i, (BetCoin / 10), _CoinType, notify.from, _PassportId, ManagePassportId);
                            }                            
                        }
                        else
                        {
                            GetMyParentRebate("1", i, BetCoin, _CoinType, notify.from, _PassportId, ManagePassportId);
                        }                        
                        #endregion
                    }
                    return retu;

                });

                Task<string> t5 = Task.Run(() =>
                {
                    //充值记录
                    {
                        RechargeCashOrder rechargeCashSelect = db.Queryable<RechargeCashOrder>().First(c => c.product_ref == notify.product_ref && c.RCType == 0);
                        if (rechargeCashSelect == null)
                        {
                            RechargeCashOrder rechargeCash = new()
                            {
                                AddTime = t,
                                UpdateTime = t,
                                IsValid = 1,
                                Sort = 1,
                                Ip = YRHelper.GetClientIPAddress(HttpContext),

                                PassportId = _PassportId,
                                OrderID = PayOrderId,
                                FromHashAddress = FromAddress,
                                ToHashAddress = ToAddress,
                                CoinNumber = BetCoin,
                                IsPayment = PayStatus,
                                CoinType = _CoinType,//币类别(0usdt 1其他)
                                RCType = 0,//充值提现(0充值 1提现)
                                ManageUserPassportId = ManagePassportId,

                                merchant_no = merchant_no,
                                sign = sign,
                                sign_type = sign_type,
                                timestamp = timestamp,
                                merchant_ref = merchant_ref,
                                system_ref = notify.system_ref,
                                amount = notify.amount,
                                pay_amount = notify.pay_amount,
                                fee = notify.fee,
                                status = notify.status,
                                success_time = notify.success_time,
                                extend_params = notify.extend_params,
                                product = notify.product,
                                product_ref = notify.product_ref,
                                block_number = notify.block_number,
                                block_hash = notify.block_hash,
                                from = notify.from,
                                to = notify.to,
                            };
                            int m = db.Insertable(rechargeCash).ExecuteCommand();
                            if (m < 1)
                            {
                                return "FAIL";
                            }
                        }
                        return retu;
                    }
                });

                Task<string> t6 = Task.Run(() =>
                {
                    //出金
                    {
                        if (IsWinFlag || OpenResult == "和" || OpenResult == "无效投注")
                        {
                            RechargeCashOrder rechargeCash = db.Queryable<RechargeCashOrder>().First(c => c.block_hash == notify.block_hash && c.block_number == notify.system_ref && c.RCType == 1);
                            if (rechargeCash == null)
                            {
                                //CashOrderId = rechargeCash.OrderID;
                                //调用出金接口
                                PayRetu payRetu = PostCash(CashOrderId, BetWinCoin, FromAddress, ManagePassportId, notify.block_hash, merchant_no, payproduct, notify.system_ref);
                                if (payRetu.code != 200)
                                {
                                    return "FAIL";
                                }
                            }
                        }
                    }
                    return retu;
                });

                await t4;
                await t5;
                await t6;
                if (t6.Result == "FAIL")
                {
                    return "";
                }
                retu = "SUCCESS";

                Task t7 = Task.Run(async () => {
                    //机器人发送消息
                    {
                        LogHelper.Warn("1");
                        string game_amount = Convert.ToDecimal(notify.pay_amount).ToString("F1");
                        if (GameDetailsName.Trim() != "庄闲" && GameDetailsName.Trim() != "百家乐")
                        {
                            game_amount = decimal.ToInt32(Convert.ToDecimal(notify.pay_amount)).ToString();
                        }
                        UserInfo bot_user = db.Queryable<UserInfo>().First(c => c.HashAddress == notify.from && c.ManageUserPassportId == ManagePassportId);
                        string win_lose_result = string.Empty;
                        if (bot_user != null)
                        {
                            long chatId = 0;
                            string str_chatId = bot_user.TgChatId;

                            string lang = "1";
                            if (!redis.Exists(str_chatId))
                            {
                                redis.Set(str_chatId, str_chatId);
                            }
                            if (string.IsNullOrWhiteSpace(redis.Get(str_chatId)))
                            {
                                redis.Set(str_chatId, str_chatId);
                            }
                            lang = redis.Get(str_chatId);
                            List<ULangDetails> list = new List<ULangDetails>();
                            if (!redis.Exists($"{ManagePassportId}Lang"))
                            {
                                list = db.Queryable<ULangDetails>().Where(c => c.ManageUserPassportId == ManagePassportId).ToList();
                                redis.Set($"{ManagePassportId}Lang", list, 3600 * 24 * 30);
                            }
                            if (redis.Get<List<ULangDetails>>($"{ManagePassportId}Lang")?.Count < 1)
                            {
                                list = db.Queryable<ULangDetails>().Where(c => c.ManageUserPassportId == ManagePassportId).ToList();
                                redis.Set($"{ManagePassportId}Lang", list, 3600 * 24 * 30);
                            }
                            list = redis.Get<List<ULangDetails>>($"{ManagePassportId}Lang");

                            string current_block_hash = notify.block_hash.Substring(notify.block_hash.Length - 5);
                            LogHelper.Debug($"GameBetCall:{GameDetailsName}");
                            win_lose_result = BotWinLoseMsg(lang, list, chatId.ToString(), IsWinFlag, GameDetailsName, CoinName, OpenResult, game_amount, current_block_hash, BetWinCoin);
                            //win_lose_result = YRHelper.url_en_de_code(true, win_lose_result);
                            LogHelper.Warn("2");
                            LogHelper.Warn(win_lose_result);
                            LogHelper.Warn(chatId.ToString());

                            if (long.TryParse(str_chatId, out chatId))
                            {
                                string botToken = $"botToken{merchant_no}";
                                if (redis.Exists($"botToken{merchant_no}"))
                                {
                                    if (redis.Get($"botToken{merchant_no}") == null)
                                    {
                                        string _botToken = db.Queryable<BotSys>().First(c => c.ManageUserPassportId == ManagePassportId)?.BotToken;

                                        redis.Set($"botToken{merchant_no}", _botToken, 60 * 60 * 24);
                                    }
                                }
                                else
                                {
                                    string _botToken = db.Queryable<BotSys>().First(c => c.ManageUserPassportId == ManagePassportId)?.BotToken;

                                    redis.Set($"botToken{merchant_no}", _botToken, 60 * 60 * 24);
                                }
                                botToken = redis.Get($"botToken{merchant_no}");

                                string bot_url = string.Empty;
                                string mark = string.Empty;
                                switch (lang)
                                {
                                    case "1":
                                        mark = "投注验证";
                                        break;
                                    case "2":
                                       mark= "投注驗證";
                                        break;
                                    case "3":
                                       mark= "Bet Verification";
                                        break;
                                    case "4":
                                        mark = "การตรวจสอบการเดิมพัน";
                                        break;
                                    default:
                                        mark = "投注验证";
                                        break;
                                }
                                InlineKeyboardMarkup inlineKeyboard = new(new[]
                                   {
                                         InlineKeyboardButton.WithUrl(
                                          text: mark,
                                        url: "https://tronscan.io/#/block/"+notify.block_number)
                                        });
                                TelegramBotClient boot = new TelegramBotClient(botToken);

                                if (IsWinFlag)
                                {
                                    

                                    Message message = boot.SendPhotoAsync(
                                                        chatId: chatId,
                                                        photo: "https://drive.google.com/file/d/1hjyvMQLgw-KZcUr8po8kub0exujWRkwb/view?usp=sharing",
                                                        caption: win_lose_result,
                                                        replyMarkup: inlineKeyboard).Result;
                                }
                                else
                                {
                                    Message message = boot.SendPhotoAsync(
                                                      chatId: chatId,
                                                      photo: "https://drive.google.com/file/d/1f3M0lem0trUGfn0b4kttRXMOIUa5jcYU/view",
                                                      caption: win_lose_result,
                                                      replyMarkup: inlineKeyboard).Result;
                                }
                                HttpHelper.Helper.GetMoths(bot_url);
                            }
                        }
                    }
                });
            }
            else
            {
                return retu;
            }

            return retu;
        }

        /// <summary>
        /// 出金回调
        /// </summary>
        /// <param name="merchant_ref"></param>
        /// <param name="product"></param>
        /// <returns></returns>
        [Route("/api/Game/GameBetCashCall")]
        [HttpPost]
        public string GameBetCashCall(string merchant_ref, string product)
        {
            string retu = "";
            MsgInfo<string> info = new();

            DateTime t = DateTime.Now;

            string merchant_no = Request.Form["merchant_no"];

            string paramss = Request.Form["params"];

            string sign = Request.Form["sign"];

            string sign_type = Request.Form["sign_type"];

            string timestamp = Request.Form["timestamp"];

            Notify noti = JsonConvert.DeserializeObject<Notify>(paramss);

            //string apikey = "e1ea605e071b38ca0bb38b1883876cb7";
            string apikey = $"merchant_no{merchant_no}";
            if (redis.Exists($"merchant_no{merchant_no}"))
            {
                if (redis.Get($"merchant_no{merchant_no}") == null)
                {
                    string _apikey = db.Queryable<PaySys>().First(c => c.merchant_no == merchant_no)?.apiKey;

                    redis.Set($"merchant_no{merchant_no}", _apikey, 60 * 60 * 24);
                }
            }
            else
            {
                string _apikey = db.Queryable<PaySys>().First(c => c.merchant_no == merchant_no)?.apiKey;

                redis.Set($"merchant_no{merchant_no}", _apikey, 60 * 60 * 24);
            }
            apikey = redis.Get($"merchant_no{merchant_no}");

            string ssssign = merchant_no + paramss + sign_type + timestamp + apikey;

            string md5ssssign = YRHelper.get_md5_32(ssssign);

            if (md5ssssign == sign)
            {
                Notify notify = JsonConvert.DeserializeObject<Notify>(paramss);
                Task<string> task = Task.Run(() =>
                {
                    int m = db.Updateable<RechargeCashOrder>().SetColumns(it => new RechargeCashOrder()
                    {
                        UpdateTime = t,
                        IsPayment = notify.status,

                        merchant_no = merchant_no,
                        sign = sign,
                        sign_type = sign_type,
                        timestamp = timestamp,
                        merchant_ref = notify.merchant_ref,
                        system_ref = notify.system_ref,
                        amount = notify.amount,
                        pay_amount = notify.pay_amount,
                        fee = notify.fee,
                        status = notify.status,
                        success_time = notify.success_time,
                        extend_params = notify.extend_params,
                        product = notify.product,
                        product_ref = notify.product_ref,
                        //block_number = notify.block_number,
                        //block_hash = notify.block_hash,
                        from = notify.from,
                        to = notify.to,
                        reversal = notify.reversal,
                        reason = notify.reason
                    }).Where(c => c.OrderID == notify.merchant_ref).ExecuteCommand();
                    if (m < 1)
                    {
                        return retu;
                    }

                    if (notify.merchant_ref.Contains("BATE"))
                    {
                        string CashOrderId = notify.merchant_ref;
                        int state = db.Updateable<Rebate>().SetColumns(c => c.UpdateTime == t)
                                .SetColumns(c => c.SettlementState == notify.status).Where(c => c.OrderID == CashOrderId).ExecuteCommand();
                        if (state < 1)
                        {
                            return retu;
                        }
                    }
                    else
                    {
                        string CashOrderId = notify.merchant_ref.Replace("C", "BET");
                        int state = db.Updateable<Bet>().SetColumns(c => c.UpdateTime == t)
                                .SetColumns(c => c.SettlementState == notify.status).Where(c => c.OrderID == CashOrderId).ExecuteCommand();
                        if (state < 1)
                        {
                            return retu;
                        }
                    }
                    return retu;
                });
                retu = "SUCCESS";
            }

            return retu;
        }

        /// <summary>
        /// 获取投注列表(UserType:0为管理员用户，1为会员用户)
        /// </summary>
        /// <param name="OpenId">OpenId</param>
        /// <param name="GameDetailsID">游戏详情ID</param>
        /// <param name="UserType"></param>
        /// <param name="UserName">用户名称</param>
        /// <param name="HashAddress">哈希地址</param>
        /// <param name="_BetCoin1">投注金额</param>
        /// <param name="_BetCoin2">投注金额</param>
        /// <param name="SettlementState">结算状态(0未结算 1已结算)</param>
        /// <param name="BetResult">投注结果(0输 1赢 2未开奖)</param>
        /// <param name="BlockHash">区块hash</param>
        /// <param name="AddTime1">开始日期</param>
        /// <param name="AddTime2">结束日期</param>
        /// <param name="PageIndex">当前页</param>
        /// <param name="PageSize">每页数量</param>
        /// <returns></returns>
        [HttpGet]
        public MsgInfo<List<NewRetuBet>> GetBet(string OpenId, string GameDetailsID, string GameName, string UserType, string UserName, string HashAddress,
            string BetCoin1, string BetCoin2, string SettlementState, string BetResult, string BlockHash, string AddTime1, string AddTime2,
            int PageIndex, int PageSize = 20)
        {
            MsgInfo<List<NewRetuBet>> info = new();
            long PassportId = IsLogin(OpenId, UserType);
            if (PassportId <= 0)
            {
                info.code = 400;
                info.msg = "Please login";
                return info;
            }
            Int64 _PassportId = 0;
            DateTime t1 = DateTime.Parse(DateTime.Now.ToString("yyyy-MM-dd") + $" 00:00:00");
            if (!string.IsNullOrWhiteSpace(AddTime1))
            {
                DateTime.TryParse(AddTime1 + $" 00:00:00", out t1);
            }
            DateTime t2 = DateTime.Parse(DateTime.Now.ToString("yyyy-MM-dd") + $" 23:59:59");
            if (!string.IsNullOrWhiteSpace(AddTime2))
            {
                DateTime.TryParse(AddTime2 + $" 23:59:59", out t2);
            }
            decimal _BetCoin1 = Convert.ToDecimal(BetCoin1);
            decimal _BetCoin2 = Convert.ToDecimal(BetCoin2);
            string block_hash = GameName;
            int _SettlementState = 0;
            int.TryParse(SettlementState, out _SettlementState);
            int _BetResult = 0;
            int.TryParse(BetResult, out _BetResult);

            int RoleId = GetRoleId(_PassportId, UserType);
            List<NewRetuBet> newRetuBets = GetMyChildsBet<NewRetuBet>(PassportId, UserType);

            if (RoleId == 1)
            {
                newRetuBets = GetMyChildsBet<NewRetuBet>(PassportId, UserType);
            }

            if (!string.IsNullOrWhiteSpace(UserName))
            {
                newRetuBets = (from c in newRetuBets where c.UserName == UserName select c).ToList();
            }
            if (!string.IsNullOrWhiteSpace(HashAddress))
            {
                newRetuBets = (from c in newRetuBets where c.HashAddress == HashAddress select c).ToList();
            }
            //下注金额
            if (!string.IsNullOrWhiteSpace(BetCoin1) && !string.IsNullOrWhiteSpace(BetCoin2))
            {
                newRetuBets = (from c in newRetuBets where c.BetCoin >= _BetCoin1 && c.BetCoin <= _BetCoin2 select c).ToList();
            }
            else if (!string.IsNullOrWhiteSpace(BetCoin1))
            {
                newRetuBets = (from c in newRetuBets where c.BetCoin >= _BetCoin1 select c).ToList();
            }
            else if (!string.IsNullOrWhiteSpace(BetCoin2))
            {
                newRetuBets = (from c in newRetuBets where c.BetCoin <= _BetCoin2 select c).ToList();
            }
            //结算状态
            if (!string.IsNullOrWhiteSpace(SettlementState))
            {
                newRetuBets = (from c in newRetuBets where c.SettlementState == _SettlementState select c).ToList();
            }
            //投注结果
            if (!string.IsNullOrWhiteSpace(BetResult))
            {
                newRetuBets = (from c in newRetuBets where c.BetResult == _BetResult select c).ToList();
            }
            //时间
            if (!string.IsNullOrWhiteSpace(AddTime1) && !string.IsNullOrWhiteSpace(AddTime2))
            {
                newRetuBets = (from c in newRetuBets where c.BetTime >= t1 && c.BetTime <= t2 select c).ToList();
            }
            else if (!string.IsNullOrWhiteSpace(AddTime1))
            {
                newRetuBets = (from c in newRetuBets where c.BetTime >= t1 select c).ToList();
            }
            else if (!string.IsNullOrWhiteSpace(AddTime2))
            {
                newRetuBets = (from c in newRetuBets where c.BetTime <= t2 select c).ToList();
            }
            info.data_count = newRetuBets.Count;
            if (PageIndex > 0)
            {
                newRetuBets = (from c in newRetuBets orderby c.BetTime descending select c).Skip((PageIndex - 1) * PageSize).Take(PageSize).ToList();
            }

            info.code = 200;
            info.msg = "success";
            info.data = newRetuBets;
            return info;
        }

        /// <summary>
        /// 获取充值提现列表-充值提现(0充值 1提现)
        /// </summary>
        /// <param name="OpenId">OpenId</param>
        /// <param name="RCType">(0充值 1提现)</param>
        /// <param name="UserType"></param>
        /// <param name="RCState">成功失败状态（0：Unpaid；1：Paid；4：Failed）</param>
        /// <param name="UserName">用户名称</param>
        /// <param name="OrderId">订单编号</param>
        /// <param name="HashAddress">哈希地址</param>
        /// <param name="AddTime1">开始日期</param>
        /// <param name="AddTime2">结束日期</param>
        /// <param name="PageIndex">当前页</param>
        /// <param name="PageSize">每页数量</param>
        /// <returns></returns>
        [HttpGet]
        public MsgInfo<List<RetuRechargeCashOrder>> GetRechargeCashOrder(string OpenId, string RCType,string RCState, string UserType, string UserName, string OrderId,
            string HashAddress, string AddTime1, string AddTime2, int PageIndex, int PageSize = 20)
        {
            MsgInfo<List<RetuRechargeCashOrder>> info = new();
            long PassportId = IsLogin(OpenId, UserType);
            if (PassportId <= 0)
            {
                info.code = 400;
                info.msg = "Please login";
                return info;
            }

            DateTime t1 = DateTime.Now;
            DateTime.TryParse(AddTime1, out t1);
            DateTime t2 = DateTime.Now;
            DateTime.TryParse(AddTime2, out t2);
            int _RCState = -1;
            int.TryParse(RCState, out _RCState);

            int[] _RCType = Array.ConvertAll<string, int>(RCType?.Split(new char[] { ',' }, StringSplitOptions.RemoveEmptyEntries) ?? new string[0], s => int.Parse(s));

            List<UserInfo> users = db.Queryable<UserInfo>().ToList();
            List<RetuRechargeCashOrder> list = null;
            //list = GetMyChildsAllLevelT<RetuRechargeCashOrder>("RechargeCashOrder", "", PassportId, UserType);
            if (UserType == "0")
            {
                list = db.Queryable<RetuRechargeCashOrder>().Where(c => c.ManageUserPassportId == PassportId).Select(c => new RetuRechargeCashOrder
                {
                    Id = c.Id
      , Ip = c.Ip
      , Sort = c.Sort
      , IsValid = c.IsValid
      , AddTime = c.AddTime
      , UpdateTime = c.UpdateTime
      , PassportId = c.PassportId
      , OrderID = c.OrderID
      , FromHashAddress = c.FromHashAddress
      , ToHashAddress = c.ToHashAddress
      , CoinNumber = c.CoinNumber
      , IsPayment = c.IsPayment
      , CoinType = c.CoinType
      , RCType = c.RCType
      , ManageUserPassportId = c.ManageUserPassportId
      , merchant_no = c.merchant_no
      , sign = c.sign
      , sign_type = c.sign_type
      , timestamp = c.timestamp
      , merchant_ref = c.merchant_ref
      , system_ref = c.system_ref
      , amount = c.amount
      , pay_amount = c.pay_amount
      , fee = c.fee
      , status = c.status
      , success_time = c.success_time
      , extend_params = c.extend_params
      , product = c.product
      , product_ref = c.product_ref
      , block_number = c.block_number
      , block_hash = c.block_hash
      , from = c.from
      , to = c.to
      , reversal = c.reversal
      , reason = c.reason
                }).ToList();
            }
            else
            {
                list = GetMyChildsAllLevelT<RetuRechargeCashOrder>("RechargeCashOrder", "", PassportId, "1");
            }

            int RoleId = GetRoleId(PassportId, UserType);
            if (RoleId == 1)
            {
                list = db.Queryable<RechargeCashOrder>().Select(c => new RetuRechargeCashOrder
                {
                    Id = c.Id
      , Ip = c.Ip
      , Sort = c.Sort
      , IsValid = c.IsValid
      , AddTime = c.AddTime
      , UpdateTime = c.UpdateTime
      , PassportId = c.PassportId
      , OrderID = c.OrderID
      , FromHashAddress = c.FromHashAddress
      , ToHashAddress = c.ToHashAddress
      , CoinNumber = c.CoinNumber
      , IsPayment = c.IsPayment
      , CoinType = c.CoinType
      , RCType = c.RCType
      , ManageUserPassportId = c.ManageUserPassportId
      , merchant_no = c.merchant_no
      , sign = c.sign
      , sign_type = c.sign_type
      , timestamp = c.timestamp
      , merchant_ref = c.merchant_ref
      , system_ref = c.system_ref
      , amount = c.amount
      , pay_amount = c.pay_amount
      , fee = c.fee
      , status = c.status
      , success_time = c.success_time
      , extend_params = c.extend_params
      , product = c.product
      , product_ref = c.product_ref
      , block_number = c.block_number
      , block_hash = c.block_hash
      , from = c.from
      , to = c.to
      , reversal = c.reversal
      , reason = c.reason
                }).ToList();
            }

            list.ForEach(c => c.username = users.Where(d => d.PassportId == c.PassportId).Select(d => d.UserName).FirstOrDefault());

            long _PassportId = 0;
            List<UserInfo> userInfos = db.Queryable<UserInfo>().ToList();
            if (!string.IsNullOrWhiteSpace(UserName))
            {
                userInfos = (from c in userInfos where c.UserName == UserName select c).ToList();
                if (userInfos.Count > 0)
                {
                    _PassportId = userInfos[0].PassportId;
                }
            }
            if (!string.IsNullOrWhiteSpace(OrderId))
            {
                list = (from c in list where c.OrderID.Contains(OrderId) select c).ToList();
            }

            if (!string.IsNullOrWhiteSpace(RCState))
            {
                list = (from c in list where c.status == _RCState select c).ToList();
            }

            if (!string.IsNullOrWhiteSpace(HashAddress))
            {
                userInfos = (from c in userInfos where c.HashAddress == HashAddress select c).ToList();
                if (userInfos.Count > 0)
                {
                    _PassportId = userInfos[0].PassportId;
                }
            }
            if (_PassportId > 0)
            {
                list = (from c in list where c.PassportId == _PassportId select c).ToList();
            }

            if (!string.IsNullOrWhiteSpace(RCType))
            {
                list = (from c in list where _RCType.Contains(c.RCType) select c).ToList();
            }
            //时间
            if (!string.IsNullOrWhiteSpace(AddTime1) && !string.IsNullOrWhiteSpace(AddTime2))
            {
                list = (from c in list where c.AddTime >= t1 && c.AddTime <= t2 select c).ToList();
            }
            else if (!string.IsNullOrWhiteSpace(AddTime1))
            {
                list = (from c in list where c.AddTime >= t1 select c).ToList();
            }
            else if (!string.IsNullOrWhiteSpace(AddTime2))
            {
                list = (from c in list where c.AddTime <= t2 select c).ToList();
            }
            info.data_count = list.Count;
            if (PageIndex > 0)
            {
                list = (from c in list orderby c.AddTime descending select c).Skip((PageIndex - 1) * PageSize).Take(PageSize).ToList();
            }

            info.code = 200;
            info.msg = "success";
            info.data = list;
            return info;
        }


        /// <summary>
        /// 获取首页充值提现Top-充值提现(0充值 1提现)
        /// </summary>
        /// <param name="OpenId">OpenId</param>
        /// <param name="RCType">(0充值 1提现)</param>
        /// <param name="GameDetailsId">游戏详情ID</param>
        /// <param name="AddTime1">开始日期</param>
        /// <param name="AddTime2">结束日期</param>
        /// <param name="UserType">(UserType:0为管理员用户，1为会员用户)</param>
        /// <param name="PageIndex"></param>
        /// <param name="PageSize"></param>
        /// <returns></returns>
        [HttpGet]
        public MsgInfo<List<IndexRechargeCashOrder>> GetRechargeTop(string OpenId, int RCType, string UserType, int GameDetailsId, string AddTime1, string AddTime2, int PageIndex, int PageSize = 20)
        {
            MsgInfo<List<IndexRechargeCashOrder>> info = new();
            long PassportId = IsLogin(OpenId, UserType);
            if (PassportId <= 0)
            {
                info.code = 400;
                info.msg = "Please login";
                return info;
            }

            //var AddTime1 = DateTime.Now.ToString("yyyy-MM-dd") + " 00:00:00";
            //var AddTime2 = DateTime.Now.ToString("yyyy-MM-dd") + " 23:59:59";
            DateTime t1 = DateTime.Now;
            DateTime.TryParse(AddTime1, out t1);
            DateTime t2 = DateTime.Now;
            DateTime.TryParse(AddTime2, out t2);
            List<IndexRechargeCashOrder> list = db.Queryable<RechargeCashOrder>().GroupBy(c => c.ToHashAddress).
                Select(c => new IndexRechargeCashOrder()
                {
                    AddTime = SqlFunc.AggregateMax(c.AddTime),
                    product = SqlFunc.AggregateMax(c.product),
                    ToHashAddress = SqlFunc.AggregateMax(c.ToHashAddress),
                    RCType = SqlFunc.AggregateMax(c.RCType),
                    CoinNumber = SqlFunc.AggregateSum(c.CoinNumber)
                }).ToList();
            List<string> adminGames = db.Queryable<AdminGameRelation>().Where(c => c.GameDetailsId == GameDetailsId && c.HashAddress != "").Select(c => c.HashAddress).ToList();
            if (RCType >= 0)
            {
                list = (from c in list where c.RCType == RCType select c).ToList();
            }
            if (GameDetailsId > 0)
            {
                if (adminGames.Count > 0)
                {
                    list = (from c in list where adminGames.Contains(c.ToHashAddress) select c).ToList();
                }
            }
            //时间
            if (!string.IsNullOrWhiteSpace(AddTime1) && !string.IsNullOrWhiteSpace(AddTime2))
            {
                list = (from c in list where c.AddTime >= t1 && c.AddTime <= t2 select c).ToList();
            }
            info.data_count = list.Count;
            if (PageIndex > 0)
            {
                list = (from c in list orderby c.AddTime descending select c).Skip((PageIndex - 1) * PageSize).Take(PageSize).ToList();
            }

            info.code = 200;
            info.msg = "success";
            info.data = list;
            return info;
        }


        /// <summary>
        /// 首页获取游戏类型的入金出金
        /// <param name="OpenId">OpenId</param>
        /// <param name="GameDetailsID">游戏详情id</param>
        /// <param name="PriceType">0:入金，1：出金，2：金额 </param>
        /// </summary>
        /// <returns></returns>
        [HttpGet]
        public MsgInfo<List<ReturnAdminUser>> GetTypePrice(string OpenId, int GameDetailsID, int PriceType)
        {
            MsgInfo<List<ReturnAdminUser>> msg = new();
            long _PassportId = IsLogin(OpenId);
            if (_PassportId <= 0)
            {
                msg.code = 400;
                msg.msg = "Please login";
                return msg;
            }
            List<ReturnAdminUser> listresult = new();
            List<ReturnAdminUser> list = new();
            ReturnAdminUser user = new();
            if (GameDetailsID == 2)
            {
                var result = db.Queryable<RechargeCashOrder>()
              .InnerJoin<AdminUser>((x, c) => x.ManageUserPassportId == c.PassportId)
              .Where((x, c) => x.IsPayment == 1 && c.GameDetailsIds.Contains(GameDetailsID.ToString()))
              .Select((x, c) => new { c.UserName, x.CoinNumber, x.RCType })
              .ToList();
                result.ForEach(x =>
                {
                    if (user != null)
                    {
                        if (user.UserName != x.UserName)
                        {
                            list.Add(user);
                            user.UserName = x.UserName;
                            user.Price = default(decimal);
                        }
                        switch (x.RCType)
                        {
                            case 0:
                                user.Price += x.CoinNumber;
                                break;
                            case 1:
                                user.Price -= x.CoinNumber;
                                break;
                        }
                    }
                    else
                    {
                        user.UserName = x.UserName;
                        switch (x.RCType)
                        {
                            case 0:
                                user.Price += x.CoinNumber;
                                break;
                            case 1:
                                user.Price -= x.CoinNumber;
                                break;
                        }

                    }
                });

            }
            else
            {
                listresult = db.Queryable<RechargeCashOrder>()
            .InnerJoin<AdminUser>((x, c) => x.ManageUserPassportId == c.PassportId)
            .Where((x, c) => x.IsPayment == 1 && x.RCType == PriceType
            && c.GameDetailsIds.Contains(GameDetailsID.ToString()))
            .Select((x, c) => new ReturnAdminUser { UserName = c.UserName, Price = x.CoinNumber })
            .ToList();
                listresult.ForEach(x =>
                {
                    if (user != null)
                    {
                        if (user.UserName != x.UserName)
                        {
                            list.Add(user);
                            user.UserName = x.UserName;
                            user.Price = default(decimal);
                        }
                        user.Price += x.Price;
                    }
                    else
                    {
                        user.UserName = x.UserName;
                        user.Price = x.Price;
                    }
                });
            }
            var rspon = list.OrderByDescending(x => x.Price).Take(7).ToList();
            msg.data = rspon;
            msg.data_count = rspon.Count();
            return msg;
        }

        /// <summary>
        /// 代理获取人数
        /// </summary>
        /// <param name="OpenId"></param>
        /// <param name="IsNow">1是获取今天的人数</param>
        /// <returns></returns>
        [HttpGet]
        public MsgInfo<List<UserInfo>> GetAgentNum(string OpenId, int IsNow, int PageIndex, int PageSize = 20)
        {
            MsgInfo<List<UserInfo>> msgInfo = new();
            long _PassportId = IsLogin(OpenId, "1");
            if (_PassportId <= 0)
            {
                msgInfo.code = 400;
                msgInfo.msg = "Please login";
                return msgInfo;
            }

            var tree = db.Queryable<UserInfo>().ToChildList(x => x.ParentId, db.Queryable<UserInfo>().Where(x => x.OpenId == OpenId).First().PassportId);
            tree = IsNow == 1 ? tree.Where(x => x.AddTime.Date == DateTime.Today).ToList() : tree;
            msgInfo.data = tree.Select(x => new UserInfo { UserName = x.UserName }).Skip((PageIndex - 1) * PageSize).Take(PageSize).ToList();
            msgInfo.data_count = tree.Count();
            return msgInfo;

        }
        /// <summary>
        ///代理 获取代理的流水
        /// </summary>
        /// <param name="OpenId"></param>
        /// <returns></returns>
        [HttpGet]
        public MsgInfo<List<ReturunUserIfon>> ProWater(string OpenId, int PageIndex, int PageSize = 20)
        {

            MsgInfo<List<ReturunUserIfon>> msg = new();
            long _PassportId = IsLogin(OpenId, "1");
            if (_PassportId <= 0)
            {
                msg.code = 400;
                msg.msg = "Please login";
                return msg;
            }
            var treeuser = db.Queryable<UserInfo>()
                .ToChildList(x => x.ParentId, db.Queryable<UserInfo>().Where(x => x.OpenId == OpenId).ToList().FirstOrDefault().PassportId);
            var user = db.Queryable<RechargeCashOrder>().Where(x => treeuser.Select(x => x.PassportId).Contains(x.PassportId) && x.IsPayment == 1).ToList();
            var result = treeuser.Select(x => new ReturunUserIfon() { UserName = x.UserName, PassportId = x.PassportId }).ToList();
            result.ForEach(x =>
            {
                user.Where(d => d.PassportId == x.PassportId).ToList().ForEach(c =>
                {
                    x.Price += c.CoinNumber;
                });
            });
            msg.data = result.Skip((PageSize - 1) * PageIndex).Take(PageIndex).ToList();
            msg.data_count = result.Count();
            return msg;
        }

        private void TreeAdmin(List<AdminUser> users, long passportid,ref List<AdminUser> adminUsers)
        {
            var result = users.Where(x => x.ParentPassportId == passportid).ToList();
            foreach (var item in result)
            {
                adminUsers.Add(item);
                TreeAdmin(users, item.PassportId, ref adminUsers);
            }
        }
        ///// <summary>
        ///// 商户获取代理人数
        ///// </summary>
        ///// <param name="OpenId"></param>
        ///// <param name="IsMer">0:商户1：代理</param>
        ///// <param name="block_hash">哈希地址</param>
        ///// <param name="sta_time">开始时间</param>
        ///// <param name="end_time">结束时间</param>
        ///// <returns></returns>
        [HttpGet]
        public MsgInfo<List<ReturnProWater>> AllProResult(string OpenId, int IsMer, string block_hash, DateTime? sta_time,
            DateTime? end_time, int PageIndex, int PageSize = 20)
        {
            MsgInfo<List<ReturnProWater>> msg = new();
            var _PassportId = IsLogin(OpenId, IsMer.ToString());
            if (_PassportId <= 0)
            {
                msg.code = 400;
                msg.msg = "Please login";
                return msg;
            }

            List<UserInfo> pro_pro = new List<UserInfo>();
            var adminuser = db.Queryable<AdminUser>().ToList();
            ///一级代理
            List<UserInfo> pro = new();
            if (IsMer == 0)
            {
              
                List<AdminUser> admins = new List<AdminUser>();
                var my = adminuser.First(x => x.OpenId == OpenId);
                TreeAdmin(adminuser, my.PassportId, ref admins);
                admins.Add(my);
               
                pro_pro = db.Queryable<UserInfo>().Where(x => admins.Select(d=>(long?)d.PassportId).Contains(x.ManageUserPassportId)&&x.RoleId!=100).ToList();
                pro = pro_pro.Where(x => x.ParentId== 999999999).WhereIF(!string.IsNullOrEmpty(block_hash), x => x.HashAddress.Contains(block_hash)).ToList();
                if (pro.Count == 0) return msg;
            }
            else
            {
                var my = db.Queryable<UserInfo>().First(d => d.OpenId == OpenId);
                pro_pro = db.Queryable<UserInfo>().Where(x =>x.ManageUserPassportId==my.ManageUserPassportId).ToList();
                pro = pro_pro.Where(x => x.ParentId == (pro_pro.FirstOrDefault(d => d.OpenId == OpenId).PassportId)).WhereIF(!string.IsNullOrEmpty(block_hash), x => x.HashAddress.Contains(block_hash)).ToList();
                if (pro.Count == 0) return msg;

            }
            var game = db.Queryable<GameType>().ToList();

            var bet=db.Queryable<Bet>().WhereIF(sta_time != null && end_time != null, x => x.AddTime >= sta_time && x.AddTime < end_time)
                .Where(x=>pro.Select(d=>d.ManageUserPassportId).Contains(x.ManageUserPassportId)&&game.Select(d=>d.Id).Contains(x.GameDetailsId)).ToList();
            List<ReturnProWater> result = new();

            foreach (var item in pro)
            {
                //查看自己有多少个代理
                List<UserInfo> _users = new();
                TreeInfo(pro_pro, item.PassportId, ref _users);
                var rechag = bet.Where(x => _users.Select(d => d.PassportId).Contains(x.PassportId)).ToList();/* GetMyChildsAllT<Bet>("bet", str, item.PassportId, "1");*/
                ReturnProWater water = new ReturnProWater()
                {
                    Id = item.Id,
                    UserName = item.UserName,
                    UseMerNum = adminuser.First(x=>x.PassportId==item.ManageUserPassportId).UserName,
                    UseProNum = _users.Count,
                    UserUrl = item.HashAddress,
                    PriceUY = rechag.Where(x => x.CoinType == 1).Sum(x => x.BetCoin),
                    PriceTY = rechag.Where(x => x.CoinType == 2).Sum(x => x.BetCoin)
                };
                ///盈利
                water.PriceUN = water.PriceUY - rechag.Where(x => x.BetResult == 1 && x.CoinType == 1).Sum(x => x.BetCoin);
                water.PriceTN = water.PriceTY - rechag.Where(x => x.BetResult == 1 && x.CoinType == 2).Sum(x => x.BetCoin);
                List<ReturnProPrices> returnPros = new List<ReturnProPrices>();
                /// 游戏的流水和盈利
                game.ForEach(x =>
                {
                    ReturnProPrices returnPro = new ReturnProPrices()
                    {
                        GameName = x.GameName,
                        PriceUY = rechag.Where(d => d.GameDetailsId == x.Id && d.CoinType == 1).Sum(d => d.BetCoin),
                        PriceTY = rechag.Where(d => d.GameDetailsId == x.Id && d.CoinType == 2).Sum(d => d.BetCoin)
                    };
                    returnPro.PriceUN = returnPro.PriceUY - rechag.Where(d => d.GameDetailsId == x.Id && d.BetResult == 0 && d.CoinType == 1).Sum(d => d.BetCoin);
                    returnPro.PriceTN = returnPro.PriceTY - rechag.Where(d => d.GameDetailsId == x.Id && d.BetResult == 1 && d.CoinType == 2).Sum(d => d.BetCoin);
                    returnPros.Add(returnPro);
                });
                water.GameType = returnPros;
                result.Add(water);
            }
            msg.data = result.Skip((PageIndex - 1) * PageSize).Take(PageSize).ToList();
            msg.data_count = result.Count;
            return msg;
        }

        private void TreeInfo(List<UserInfo> list, long PassportId, ref List<UserInfo> infos)
        {
            var result = list.Where(x => x.ParentId == PassportId).ToList();
            foreach (var item in result)
            {
                TreeInfo(list, item.PassportId, ref infos);
                infos.Add(item);
            }
        }
        /// <summary>
        /// 商户查看代理日志
        /// </summary>
        /// <param name="Id"></param>
        /// <param name="IsMer"></param>
        /// <param name="sta_time"></param>
        /// <param name="end_time"></param>
        /// <param name="PageIndex"></param>
        /// <param name="PageSize"></param>
        /// <returns></returns>
        [HttpGet]
        public MsgInfo<List<ReturunProLV>> GetMerPro(int Id, int IsMer, DateTime? sta_time, DateTime? end_time, int PageIndex, int PageSize = 20)
        {

            List<AdminUser> pro_merchants = new List<AdminUser>();//直属
            List<UserInfo> pro_pro = new();//个人直属
            List<long> passsid = new List<long>();
            var all_pro = new List<UserInfo>();
            if (IsMer == 0)
            {
                var myuser = db.Queryable<AdminUser>().Where(x => x.Id == Id).First();
                all_pro = db.Queryable<UserInfo>().Where(x => x.ManageUserPassportId == myuser.PassportId).ToList() ;
                //直属代理
                passsid.Add(myuser.PassportId);
                ProProNum(all_pro, passsid, new long(), ref pro_pro);
            }
            else
            {
                var my = db.Queryable<UserInfo>().Where(x => x.Id == Id).First();
                all_pro = db.Queryable<UserInfo>().Where(d => d.ManageUserPassportId == my.ManageUserPassportId).ToList();
                ProProNum(all_pro, passsid, my.PassportId, ref pro_pro);
                passsid.Add(my.PassportId);
            }

            List<RechargeCashOrder> all_water = new List<RechargeCashOrder>();

            all_water = db.Queryable<RechargeCashOrder>().WhereIF(sta_time != null, x => x.AddTime >= sta_time)
            .WhereIF(end_time != null, x => x.AddTime < end_time)
            .Where(x => x.IsPayment == 1 && pro_pro.Select(d => d.PassportId).ToList().Contains(x.PassportId)).ToList();

            var game_type = db.Queryable<GameType>().ToList();
            //获取代理关联的x  游戏详表
            var pro_game = db.Queryable<Bet>().Where(x => pro_pro.Select(d => d.PassportId).Contains(x.PassportId)).ToList();


            //返佣金额
            var returnamount = db.Queryable<RebateDetails>().Where(x => pro_pro.Select(d => d.PassportId).ToList().Contains(x.BetPassportId)).ToList();
            var rtnpro = db.Queryable<Sys>().Where(x => x.Keys == "RebatePct").First();

            List<ReturunProLV> plv = new List<ReturunProLV>();

            ProLvPro(pro_pro, passsid, 2, all_water, game_type, returnamount, rtnpro, ref plv);

            MsgInfo<List<ReturunProLV>> msg = new();
            msg.data = plv.Skip((PageIndex - 1) * PageSize).Take(PageSize).ToList();
            msg.data_count = plv.Count;
            return msg;

        }
        private void ProLvPro(List<UserInfo> list, List<long> passportid, int Is, List<RechargeCashOrder> all_water,
            List<GameType> game_type, List<RebateDetails> rtnamount,
            Sys rtnpro, ref List<ReturunProLV> plv)
        {
            var result = list.Where(x => passportid.Contains(x.ParentId)).ToList();
            foreach (var item in result)
            {
                ReturunProLV proLV = new ReturunProLV();
                proLV.Id = item.Id;
                proLV.UserName = item.UserName;
                proLV.Level = "LV" + Is;
                proLV.HashAddress = item.HashAddress;
                proLV.RtnBfb = rtnpro.Value;
                proLV.RtnAmount = rtnamount.Where(x => x.BetPassportId == item.PassportId).Sum(x => x.RebateAmount);

                var Y1 = all_water.Where(x => x.RCType == 0 && x.CoinType == 0 && x.PassportId == item.PassportId).Sum(x => x.CoinNumber);
                proLV.UsdtY = "USDT: " + Y1;

                var N1 = Y1 - (all_water.Where(x => x.RCType == 0 && x.CoinType == 1 && x.PassportId == item.PassportId).Sum(x => x.CoinNumber));

                proLV.UsdtN = "USDT: " + N1;
                var Y2 = all_water.Where(x => x.RCType == 0 && x.CoinType == 1 && x.PassportId == item.PassportId).Sum(x => x.CoinNumber);
                plv.Add(proLV);
                var N2 = Y2 - all_water.Where(x => x.RCType == 1 && x.CoinType == 1 && x.PassportId == item.PassportId).Sum(x => x.CoinNumber);
                if (Y2 != 0 && N2 != 0)
                {
                    var TRX = new ReturunProLV()
                    {
                        RtnBfb = proLV.RtnBfb,
                        HashAddress = proLV.HashAddress,
                        Id = proLV.Id,
                        Level = proLV.Level,
                        RtnAmount = proLV.RtnAmount,
                        UsdtY = "TRX: " + Y2,
                        UsdtN = "TRX: " + N2,
                        UserName = proLV.UserName
                    };
                    plv.Add(TRX);
                }

                Is++;
                passportid.Clear();
                passportid.Add(item.PassportId);
                ProLvPro(list, passportid, Is, all_water, game_type, rtnamount, rtnpro, ref plv);
                Is--;
            }
            Is--;
        }
        ///// <summary>
        ///// 获取代理的数量
        ///// </summary>
        ///// <param name="list"></param>
        ///// <param name="Man_PassportID"></param>
        ///// <param name="PassportId"></param>
        ///// <param name="num"></param>
        private void ProProNum(List<UserInfo> list, List<long> Man_PassportID, long? PassportId, ref List<UserInfo> num)
        {
            List<UserInfo> user = new();
            if (Man_PassportID.Count > 0)
                user = list.Where(x => Man_PassportID.Contains((long)x.ManageUserPassportId)).ToList();
            else
                user = list.Where(x => x.ParentId == PassportId).ToList();
            foreach (var item in user)
            {
                ProProNum(list, new List<long>(), item.PassportId, ref num);
                num.Add(item);
            }
        }

        /// <summary>
        /// 俱乐部详情
        /// </summary>
        /// <param name="OpenId"></param>
        /// <param name="IsMer">0:商户1代理</param>
        /// <returns></returns>
        [HttpGet]
        public MsgInfo<ReturnMerDetail> MerDetail(string OpenId, int IsMer)
        {
            MsgInfo<ReturnMerDetail> msg = new();
            long _PassportId = IsLogin(OpenId, IsMer.ToString());
            if (_PassportId <= 0)
            {
                msg.code = 400;
                msg.msg = "Please login";
                return msg;
            }
            ReturnMerDetail result = new ReturnMerDetail();
            //商户代理
            List<UserInfo> pro_pro = new();
            List<AdminUser> mer_user = new();
            var userInfos = new List<UserInfo>();// db.Queryable<UserInfo>().ToList();
            if (IsMer == 0)
            {
                var meruser = db.Queryable<AdminUser>().ToList();
                var my = meruser.Where(x => x.OpenId == OpenId).First();

                mer_user.Add(my);
                ProMerNum(meruser, my.PassportId, ref mer_user);
                userInfos = db.Queryable<UserInfo>().Where(d=>d.ManageUserPassportId==my.PassportId)
                    .WhereIF(mer_user.Count > 0, x => mer_user.Select(d => d.PassportId).ToList().Contains((long)x.ManageUserPassportId)).ToList();
                //商户
                ProProNum(userInfos, mer_user.Select(x => x.PassportId).ToList(), 0, ref pro_pro);

                result.MerNum = mer_user.Count;
                result.TodayMerNum = mer_user.Where(x => x.AddTime >= DateTime.Today).Count();
                result.ProNum = pro_pro.Count;
                result.TodayProNum = pro_pro.Where(x => x.AddTime >= DateTime.Today).Count();
            }
            else
            {
                var my = db.Queryable<UserInfo>().First(d => d.OpenId == OpenId);
                userInfos = db.Queryable<UserInfo>().Where(d => d.ManageUserPassportId == my.ManageUserPassportId).ToList();
                //一级代理
                var prouser = userInfos.Where(x => x.ParentId == (userInfos.Where(x => x.OpenId == OpenId).FirstOrDefault().PassportId));
                //商户总人数    
                result.MerNum = prouser.Count();
                var longs = new List<long>();
                //一级代理之下的是有代理
                prouser.ToList().ForEach(x =>
                {
                    ProProNum(userInfos, longs, x.PassportId, ref pro_pro);
                });
                result.ProNum = pro_pro.Count;
                result.TodayMerNum = prouser.Where(x => x.AddTime.Date >= DateTime.Today).Count();
                result.TodayProNum = pro_pro.Where(x => x.AddTime.Date >= DateTime.Today).Count();
                pro_pro.AddRange(prouser);
            }
            //俱乐部总人数
            result.PeoNum = result.MerNum + result.ProNum;
            result.TodayPeoNum = result.TodayMerNum + result.TodayProNum;
            string str = "'" + DateTime.Today.AddDays(-1).ToString("yyyy-MM-dd HH:mm") + "'";

            List<Bet> bet = db.Queryable<Bet>().Where(d => pro_pro.Select(s => s.PassportId).Contains(d.PassportId)).ToList();
            //new List<Bet>();
            //pro_pro.ForEach(d =>
            //{
            //    bet.AddRange(GetMyChildsAllT<Bet>("bet", "", d.PassportId, "1"));
            //});

            result.TodayUsdt = bet.Where(x => x.AddTime >= DateTime.Today && x.BetResult != 2).Sum(x => x.BetCoin);

            result.TodayGameNum = bet.Where(x => x.AddTime >= DateTime.Today && x.BetResult != 2).Count();
            result.TodayNum = bet.Where(x => x.AddTime >= DateTime.Today).Select(x => new { Admin = x.ManageUserPassportId, Info = x.PassportId }).Distinct().Count();
            result.YesterdayUsdt = bet.Where(x => x.AddTime >= DateTime.Now.Date.AddDays(-1) && x.AddTime < DateTime.Today && x.BetResult != 2).Sum(x => x.BetCoin);
            //result.YesterdayTrx = all_water.Where(x => x.AddTime >= DateTime.Now.Date.AddDays(-1) && x.AddTime < DateTime.Today && x.CoinType == 1).Sum(x => x.CoinNumber);
            result.YesterdayGameNum = bet.Where(x => x.AddTime >= DateTime.Today.AddDays(-1) && x.AddTime < DateTime.Today).Count();
            result.YesterdayNum = bet.Where(x => x.AddTime >= DateTime.Today.AddDays(-1) && x.AddTime < DateTime.Today).Select(x => new { Admin = x.ManageUserPassportId, Info = x.PassportId }).Distinct().Count();


            result.TodayUsdtY = result.TodayUsdt - bet.Where(d => d.BetResult == 0 && d.AddTime >= DateTime.Today).Sum(d => d.BetCoin);

            result.YesterdayUsdtY = result.YesterdayUsdt - bet.Where(x => x.AddTime >= DateTime.Today.AddDays(-1) && x.AddTime < DateTime.Today && x.BetResult == 0).Sum(d => d.BetCoin);

            msg.data = result;
            return msg;
        }
        /// <summary>
        /// 获取商户的数量
        /// </summary>
        /// <param name="list"></param>
        /// <param name="PassportId"></param>
        /// <param name="num"></param>
        private void ProMerNum(List<AdminUser> list, long? PassportId, ref List<AdminUser> num)
        {
            var result = list.Where(x => x.ParentPassportId == PassportId).ToList();
            foreach (var item in result)
            {
                ProMerNum(list, item.PassportId, ref num);
                num.Add(item);
            }
        }

        /// <summary>
        /// 添加/修改机器人
        /// </summary>
        /// <returns></returns>
        [HttpPost]
        public MsgInfo<string> PostBot(dynamic param)
        {
            MsgInfo<string> info = new();
            DateTime t = DateTime.Now;
            var data = JsonConvert.DeserializeObject<dynamic>(param.ToString());
            //string OpenId = data.OpenId.ToString();
            //if (Object.ReferenceEquals(null, data))
            //{
            //    info.code = 400;
            //    info.msg = "Network Exception";
            //    return info;
            //}
            //long _PassportId = IsLogin(OpenId);
            //if (_PassportId <= 0)
            //{
            //    info.code = 400;
            //    info.msg = "Please login";
            //    return info;
            //}

            //string BotToken = data.BotToken.ToString();
            //string Details = data.Details.ToString();
            //int IsStart = int.Parse(data.IsStart.ToString());
            //long ManageUserPassportId = long.Parse(data.ManageUserPassportId.ToString());
            //int BotId = int.Parse(data.BotId.ToString());

            //if (BotId < 1)
            //{
            //    BotSys bot = new()
            //    {
            //        AddTime = t,
            //        UpdateTime = t,
            //        IsValid = 1,
            //        Sort = 1,
            //        Ip = YRHelper.GetClientIPAddress(HttpContext),

            //        BotToken = BotToken,
            //        Details = Details,
            //        IsStart = IsStart,
            //        ManageUserPassportId = _PassportId
            //    };
            //}
            //else
            //{
            //    int i = db.Updateable<BotSys>().SetColumns(c => c.UpdateTime == t).
            //        SetColumns(c => c.BotToken == BotToken).
            //        SetColumns(c => c.Details == Details).
            //        SetColumns(c => c.IsStart == IsStart).
            //        Where(c => c.Id == BotId).ExecuteCommand();
            //    if (i < 1)
            //    {
            //        info.code = 400;
            //        info.msg = "Network Exception";
            //        return info;
            //    }
            //}

            ////（0启动，1关闭）
            //if (IsStart == 0)
            {
                string strCmdText = System.AppDomain.CurrentDomain.SetupInformation.ApplicationBase;
                //System.Diagnostics.Process process = System.Diagnostics.Process.Start(strCmdText + "Bot\\Telegram_Bot.exe");
                //System.Diagnostics.Process process = System.Diagnostics.Process.Start("calc.exe");
                System.Diagnostics.Process process = System.Diagnostics.Process.Start(strCmdText + "Bot\\Telegram_Bot.exe");

                //ProcessStartInfo psi = new ProcessStartInfo();

                //psi.FileName = strCmdText + "Bot\\Telegram_Bot.exe";

                //psi.UseShellExecute = false;

                //psi.WorkingDirectory = strCmdText + "Bot";

                //psi.CreateNoWindow = true;

                //Process.Start(psi);
            }

            info.code = 200;
            info.msg = "success";

            return info;
        }

        /// <summary>
        /// 添加/修改支付商户(PayId:""为添加，有值为修改)
        /// </summary>
        /// <param name="param">
        /// {"OpenId":"9e26f6e2244f7e34ed8b4eabdcb2f9c4","merchant_no":"支付商户号","merchant_name":"商户名称","product":"产品类型","apiKey":"apiKey","RebateRatio":"1","RebateParentRatio":"50","SettlemnetFeeRatio":"5","request_url1":"","request_url2":"","request_url3":"","PassportId","","PayId":""}
        /// </param>
        /// <returns></returns>
        [HttpPost]
        public MsgInfo<string> PostPaySys(dynamic param)
        {
            MsgInfo<string> info = new();
            DateTime t = DateTime.Now;
            var data = JsonConvert.DeserializeObject<dynamic>(param.ToString());
            string OpenId = data.OpenId.ToString();
            if (Object.ReferenceEquals(null, data))
            {
                info.code = 400;
                info.msg = "Network Exception";
                return info;
            }
            long _ManageUserPassportId = IsLogin(OpenId);
            if (_ManageUserPassportId <= 0)
            {
                info.code = 400;
                info.msg = "Please login";
                return info;
            }

            string merchant_no = data.merchant_no.ToString();
            string merchant_name = data.merchant_name.ToString();
            string product = data.product.ToString();
            string apiKey = data.apiKey.ToString();
            string RebateRatio = data.RebateRatio.ToString();
            string RebateParentRatio = data.RebateParentRatio.ToString();
            string SettlemnetFeeRatio = data.SettlemnetFeeRatio.ToString();
            string request_url1 = data.request_url1.ToString();
            string request_url2 = data.request_url2.ToString();
            string request_url3 = data.request_url3.ToString();
            string PassportId = data.PassportId.ToString();
            long _PassportId = 0;
            long.TryParse(PassportId, out _PassportId);
            decimal _RebateRatio = 1;
            decimal.TryParse(RebateRatio, out _RebateRatio);
            decimal _RebateParentRatio = 50;
            decimal.TryParse(RebateParentRatio, out _RebateParentRatio);
            decimal _SettlemnetFeeRatio = 1;
            decimal.TryParse(SettlemnetFeeRatio, out _SettlemnetFeeRatio);
            string PayId = data.PayId.ToString();
            if (string.IsNullOrEmpty(PayId))
            {
                PaySys role = new()
                {
                    AddTime = t,
                    UpdateTime = t,
                    IsValid = 1,
                    Sort = 1,
                    Ip = YRHelper.GetClientIPAddress(HttpContext),

                    merchant_no = merchant_no,
                    merchant_name = merchant_name,
                    product = product,
                    apiKey = apiKey,
                    RebateRatio = _RebateRatio,
                    RebateParentRatio = _RebateParentRatio,
                    SettlemnetFeeRatio = _SettlemnetFeeRatio,

                    request_url1 = request_url1,
                    request_url2 = request_url2,
                    request_url3 = request_url3,
                    PassportId = _PassportId,
                    ManageUserPassportId = _ManageUserPassportId
                };
                int m = db.Insertable(role).ExecuteCommand();
                if (m < 1)
                {
                    info.code = 400;
                    info.msg = "Network Exception";
                    return info;
                }
            }
            else
            {
                int _PayId = 0;
                int.TryParse(PayId, out _PayId);
                int m = db.Updateable<PaySys>().SetColumns(c => c.UpdateTime == t).
                    SetColumns(c => c.merchant_no == merchant_no).
                    SetColumns(c => c.merchant_name == merchant_name).
                    SetColumns(c => c.product == product).
                    SetColumns(c => c.apiKey == apiKey).
                    SetColumns(c => c.RebateRatio == _RebateRatio).
                    SetColumns(c => c.RebateParentRatio == _RebateParentRatio).
                    SetColumns(c => c.SettlemnetFeeRatio == _SettlemnetFeeRatio).
                    SetColumns(c => c.request_url1 == request_url1).
                    SetColumns(c => c.request_url2 == request_url2).
                    SetColumns(c => c.request_url3 == request_url3).
                    Where(c => c.Id == _PayId).ExecuteCommand();
                if (m < 1)
                {
                    info.code = 400;
                    info.msg = "Network Exception";
                    return info;
                }
            }


            info.code = 200;
            info.msg = "success";

            return info;
        }

        /// <summary>
        /// 获取支付商户
        /// </summary>
        /// <param name="OpenId">OpenId</param>
        /// <param name="merchant_no">支付商户号</param>
        /// <param name="PageIndex"></param>
        /// <param name="PageSize"></param>
        /// <returns></returns>
        [HttpGet]
        public MsgInfo<List<PaySys>> GetPaySys(string OpenId, string merchant_no, int PageIndex, int PageSize = 20)
        {
            MsgInfo<List<PaySys>> info = new();
            long _PassportId = IsLogin(OpenId);
            if (_PassportId <= 0)
            {
                info.code = 400;
                info.msg = "Please login";
                return info;
            }
            List<PaySys> list = db.Queryable<PaySys>().ToList();
            int _RoleId = GetRoleId(_PassportId);
            if (_RoleId > 1)
            {
                list = (from c in list where c.PassportId == _PassportId select c).ToList();
            }

            if (!string.IsNullOrWhiteSpace(merchant_no))
            {
                list = (from c in list where c.merchant_no == merchant_no select c).ToList();
            }
            info.data_count = list.Count;
            if (PageIndex > 0)
            {
                list = (from c in list orderby c.AddTime descending select c).Skip((PageIndex - 1) * PageSize).Take(PageSize).ToList();
            }

            info.code = 200;
            info.msg = "success";
            info.data = list;
            return info;
        }


        /// <summary>
        /// 发送机器人消息
        /// </summary>
        /// <param name="lang">语言</param>
        /// <param name="list">语言集合</param>
        /// <param name="UserName">机器人聊天ID</param>
        /// <param name="IsWinFlag">是否中奖</param>
        /// <param name="GameDetailsName">游戏名称</param>
        /// <param name="CoinName">币种类别名称</param>
        /// <param name="OpenResult">中奖结果</param>
        /// <param name="game_amount">游戏金额</param>
        /// <param name="block_hash">hashblock</param>
        /// <param name="BetWinCoin">中奖金额</param>
        /// <returns></returns>
        private string BotWinLoseMsg(string lang, List<ULangDetails> list, string UserName, bool IsWinFlag, string GameDetailsName, string CoinName, string OpenResult, string game_amount, string block_hash, decimal BetWinCoin)
        {
            int IntLang = 1;
            if (!string.IsNullOrWhiteSpace(lang))
            {
                int.TryParse(lang, out IntLang);
            }
            string GAMEWIN = (from c in list where c.ULangKey == "GAMEWIN" && c.ULangId == IntLang select c.ULangValue).FirstOrDefault();
            string GAMELOSE = (from c in list where c.ULangKey == "GAMELOSE" && c.ULangId == IntLang select c.ULangValue).FirstOrDefault();
            string GAMEDRAW = (from c in list where c.ULangKey == "GAMEDRAW" && c.ULangId == IntLang select c.ULangValue).FirstOrDefault();
            string GAMEINVALIDBET = (from c in list where c.ULangKey == "GAMEINVALIDBET" && c.ULangId == IntLang select c.ULangValue).FirstOrDefault();

            string YXLX = string.Empty;
            if (GameDetailsName.Contains("大小"))
            {
                YXLX = (from c in list where c.ULangKey == "DXYX" && c.ULangId == IntLang select c.ULangValue).FirstOrDefault();
            }
            if (GameDetailsName.Contains("单双"))
            {
                YXLX = (from c in list where c.ULangKey == "DSYX" && c.ULangId == IntLang select c.ULangValue).FirstOrDefault();
            }
            if (GameDetailsName.Contains("尾数"))
            {
                YXLX = (from c in list where c.ULangKey == "SWYX" && c.ULangId == IntLang select c.ULangValue).FirstOrDefault();
            }
            if (GameDetailsName.Contains("庄闲"))
            {
                YXLX = (from c in list where c.ULangKey == "BJLYX" && c.ULangId == IntLang select c.ULangValue).FirstOrDefault();
            }

            string win_lose_result = string.Empty;
            if (IsWinFlag)
            {
                win_lose_result = GAMEWIN;
            }
            else
            {
                win_lose_result = GAMELOSE;
            }
            if (OpenResult.ToUpper().Trim() == "和")
            {
                win_lose_result = GAMEDRAW;
            }

            if (OpenResult.ToUpper().Trim() == "无效投注")
            {
                win_lose_result = GAMEINVALIDBET;
            }

            win_lose_result = win_lose_result.Replace("GameDetailsName", YXLX).
                Replace("game_amount", game_amount).
                Replace("block_hash", block_hash).
                Replace("BetWinCoin", BetWinCoin.ToString()).
                Replace("USDT", CoinName).
                Replace("GAMETIME", DateTime.Now.ToString());
            return win_lose_result;
            
        }


        #region 数据统计

        /// <summary>
        /// 获取交易统计(UserType:0为管理员用户，1为会员用户)
        /// </summary>
        /// <param name="OpenId"></param>
        /// <param name="UserType"></param>
        /// <param name="AgentPassportId">代理PassportId</param>
        /// <param name="AddTime1"></param>
        /// <param name="AddTime2"></param>
        /// <returns></returns>
        [HttpGet]
        public MsgInfo<RetuTradeStatis> GetTradeDataStatis(string OpenId, string UserType, string AgentPassportId, string AddTime1, string AddTime2)
        {
            MsgInfo<RetuTradeStatis> info = new();
            DateTime t = DateTime.Now;
            long _PassportId = IsLogin(OpenId, UserType);
            if (_PassportId <= 0)
            {
                info.code = 400;
                info.msg = "Please login";
                return info;
            }
            if (!string.IsNullOrWhiteSpace(AgentPassportId))
            {
                UserType = "1";
                long.TryParse(AgentPassportId, out _PassportId);
            }

            //t1
            DateTime t1 = DateTime.Parse(DateTime.Now.ToString("yyyy-MM-dd") + $" 00:00:00");
            if (!string.IsNullOrWhiteSpace(AddTime1))
            {
                DateTime.TryParse(AddTime1 + $" 00:00:00", out t1);
            }
            DateTime t2 = DateTime.Parse(DateTime.Now.ToString("yyyy-MM-dd") + $" 23:59:59");
            if (!string.IsNullOrWhiteSpace(AddTime2))
            {
                DateTime.TryParse(AddTime2 + $" 23:59:59", out t2);
            }
            //充值提现(0充值 1提现)
            //手续费类型（0充值，1提款，2优惠红利，3反水派发，4代理佣金，5代理手续费 ）
            //损益 =（收入 - 支出）
            //收入 = 存款
            //支出 = 取款 + 优惠红利 + 流水返佣
            RetuTradeStatis tradeStatis = new RetuTradeStatis();
            
            List<RechargeCashOrder> r1 = GetMyChildsRCOrder<RechargeCashOrder>(_PassportId, " and b.RCType=0 ", UserType);
            List<RechargeCashOrder> r2 = GetMyChildsRCOrder<RechargeCashOrder>(_PassportId, " and b.RCType=1 ", UserType);
            List<RechargeCashOrder> r3 = GetMyChildsRCOrder<RechargeCashOrder>(_PassportId, " and b.RCType=2 ", UserType);
            List<RechargeCashOrder> r4 = GetMyChildsRCOrder<RechargeCashOrder>(_PassportId, " and b.RCType=3 ", UserType);
            List<NewRetuBet> betsSource = GetMyChildsBet<NewRetuBet>(_PassportId, UserType);
            List<FeeOrder> feeOrders1Source = GetMyChildsAllT<FeeOrder>("FeeOrder", " and b.FeeType=1", _PassportId, UserType);

            List<RechargeCashOrder> rc1 = r1;//充值
            List<RechargeCashOrder> rc2 = r2;//提现
            List<RechargeCashOrder> rc3 = r3;//活动
            List<RechargeCashOrder> rc4 = r4;//返佣
            List<NewRetuBet> bets1 = (from c in betsSource where c.OpenResult=="无效投注" select c).ToList();//无效投注
            List<NewRetuBet> bets2 = betsSource;//投注笔数
            List<FeeOrder> feeOrders1 = feeOrders1Source;//流水手续费

            List<RechargeCashOrder> rc1TB = new List<RechargeCashOrder>();
            List<RechargeCashOrder> rc2TB = new List<RechargeCashOrder>();
            List<RechargeCashOrder> rc3TB = new List<RechargeCashOrder>();
            List<RechargeCashOrder> rc4TB = new List<RechargeCashOrder>();
            List<NewRetuBet> bets1TB = new List<NewRetuBet>();
            List<NewRetuBet> bets2TB = new List<NewRetuBet>();
            List<FeeOrder> feeOrders1TB = new List<FeeOrder>();

            //时间
            if (!string.IsNullOrWhiteSpace(AddTime1) && !string.IsNullOrWhiteSpace(AddTime2))
            {
                DateTime at1 = DateTime.Now;
                DateTime.TryParse(AddTime1 + $" 00:00:00", out at1);
                DateTime at2 = DateTime.Now;
                DateTime.TryParse(AddTime2 + $" 00:00:00", out at2);
                double TM = (at2.AddDays(1) - at1).TotalMinutes;
                DateTime t3 = t1.AddMinutes(-TM);

                rc1 = (from c in rc1 where c.AddTime >= t1 && c.AddTime <= t2 select c).ToList();
                rc2 = (from c in rc2 where c.AddTime >= t1 && c.AddTime <= t2 select c).ToList();
                rc3 = (from c in rc3 where c.AddTime >= t1 && c.AddTime <= t2 select c).ToList();
                rc4 = (from c in rc4 where c.AddTime >= t1 && c.AddTime <= t2 select c).ToList();
                bets1 = (from c in bets1 where c.BetTime >= t1 && c.BetTime <= t2 select c).ToList();
                bets2 = (from c in bets2 where c.BetTime >= t1 && c.BetTime <= t2 select c).ToList();
                feeOrders1 = (from c in feeOrders1 where c.AddTime >= t1 && c.AddTime <= t2 select c).ToList();                

                rc1TB = (from c in rc1 where c.AddTime >= t3 && c.AddTime <= t1 select c).ToList();
                rc2TB = (from c in rc2 where c.AddTime >= t3 && c.AddTime <= t1 select c).ToList();
                rc3TB = (from c in rc3 where c.AddTime >= t3 && c.AddTime <= t1 select c).ToList();
                rc4TB = (from c in rc4 where c.AddTime >= t3 && c.AddTime <= t1 select c).ToList();
                bets1TB = (from c in bets1 where c.BetTime >= t3 && c.BetTime <= t1 select c).ToList();
                bets2TB = (from c in bets2 where c.BetTime >= t3 && c.BetTime <= t1 select c).ToList();
                feeOrders1TB = (from c in feeOrders1 where c.AddTime >= t3 && c.AddTime <= t1 select c).ToList();              
            }
            else if (!string.IsNullOrWhiteSpace(AddTime1))
            {
                rc1 = (from c in rc1 where c.AddTime >= t1 select c).ToList();
                rc2 = (from c in rc2 where c.AddTime >= t1 select c).ToList();
                rc3 = (from c in rc3 where c.AddTime >= t1 select c).ToList();
                rc4 = (from c in rc4 where c.AddTime >= t1 select c).ToList();
                bets1 = (from c in bets1 where c.BetTime >= t1 select c).ToList();
                bets2 = (from c in bets2 where c.BetTime >= t1 select c).ToList();
                feeOrders1 = (from c in feeOrders1 where c.AddTime >= t1 select c).ToList();                
            }
            else if (!string.IsNullOrWhiteSpace(AddTime2))
            {
                rc1 = (from c in rc1 where c.AddTime <= t2 select c).ToList();
                rc2 = (from c in rc2 where c.AddTime <= t2 select c).ToList();
                rc3 = (from c in rc3 where c.AddTime <= t2 select c).ToList();
                rc4 = (from c in rc4 where c.AddTime <= t2 select c).ToList();
                bets1 = (from c in bets1 where c.BetTime <= t2 select c).ToList();
                bets2 = (from c in bets2 where c.BetTime <= t2 select c).ToList();
                feeOrders1 = (from c in feeOrders1 where c.AddTime <= t2 select c).ToList();                
            }

            tradeStatis.AllWinLose =
                rc1.Sum(c => c.CoinNumber) - rc2.Sum(c => c.CoinNumber) - rc3.Sum(c => c.CoinNumber) - rc4.Sum(c => c.CoinNumber);
            tradeStatis.AllRecharge = rc1.Sum(c => c.CoinNumber);
            tradeStatis.AllCash = rc2.Sum(c => c.CoinNumber);
            tradeStatis.AllDiscount = rc3.Sum(c => c.CoinNumber);
            tradeStatis.AllRebate = rc4.Sum(c => c.CoinNumber);
            tradeStatis.AllRebateFee = feeOrders1.Sum(c => c.FeeAmount);
            tradeStatis.AllInvalidAmount = bets1.Sum(c=>c.BetCoin);
            tradeStatis.AllGameCount = bets2.Count;

            if (!string.IsNullOrWhiteSpace(AddTime1) && !string.IsNullOrWhiteSpace(AddTime2))
            {
                var AllWinLoseTB = 
                    rc1TB.Sum(c => c.CoinNumber) - rc2TB.Sum(c => c.CoinNumber) - rc3TB.Sum(c => c.CoinNumber) - rc4TB.Sum(c => c.CoinNumber);
                var AllRechargeTB = rc1TB.Sum(c => c.CoinNumber);
                var AllCashTB = rc2TB.Sum(c => c.CoinNumber);
                var AllDiscountTB = rc3TB.Sum(c => c.CoinNumber);
                var AllRebateTB = rc4TB.Sum(c => c.CoinNumber);
                var AllRebateFeeTB = bets1TB.Sum(c => c.BetCoin);
                var AllInvalidAmountTB = feeOrders1TB.Sum(c => c.FeeAmount);                
                var AllGameCountTB = bets2TB.Count;

                tradeStatis.AllWinLoseTB = AllWinLoseTB == 0 ? "0%" : ((tradeStatis.AllWinLose - AllWinLoseTB) / AllWinLoseTB * 100).ToString("F6") + "%";
                tradeStatis.AllRechargeTB = AllRechargeTB == 0 ? "0%" : ((tradeStatis.AllRecharge - AllRechargeTB) / AllRechargeTB * 100).ToString("F6") + "%";
                tradeStatis.AllCashTB = AllCashTB == 0 ? "0%" : ((tradeStatis.AllCash - AllCashTB) / AllCashTB * 100).ToString("F6") + "%";
                tradeStatis.AllDiscountTB = AllDiscountTB == 0 ? "0%" : ((tradeStatis.AllDiscount - AllDiscountTB) / AllDiscountTB * 100).ToString("F6") + "%";
                tradeStatis.AllRebateTB = AllRebateTB == 0 ? "0%" : ((tradeStatis.AllRebate - AllRebateTB) / AllRebateTB * 100).ToString("F6") + "%";
                tradeStatis.AllRebateFeeTB = AllRebateFeeTB == 0 ? "0%" : ((tradeStatis.AllRebateFee - AllRebateFeeTB) / AllRebateFeeTB * 100).ToString("F6") + "%";
                tradeStatis.AllInvalidAmountTB = AllInvalidAmountTB == 0 ? "0%" : ((tradeStatis.AllInvalidAmount - AllInvalidAmountTB) / AllInvalidAmountTB * 100).ToString("F6") + "%";
                tradeStatis.AllGameCountTB = AllGameCountTB == 0 ? "0%" : ((tradeStatis.AllGameCount - AllGameCountTB) / AllGameCountTB * 100).ToString("F6") + "%";
            }

            #region 游戏占比
            List<GameRatio> gameRatios = new List<GameRatio>();
            decimal allBetCoin = bets2.Sum(c => c.BetCoin);

            var game_ratio = (from c in bets2 group c by c.GameName into g select new { GameName = g.Key, BetCoin = g.Sum(c => c.BetCoin) }).ToList();

            foreach (var item in game_ratio)
            {
                var GameName = item.GameName;
                var GameAmount = item.BetCoin;
                var GameAmountRation = allBetCoin == 0 ? "0%" : (item.BetCoin / allBetCoin * 100).ToString("F6") + "%";

                GameRatio ratio = new GameRatio();
                ratio.x = $"{GameName} {GameAmount} {GameAmountRation}";
                ratio.y = item.BetCoin;
                gameRatios.Add(ratio);
            }
            tradeStatis.gameRatios = gameRatios;
            #endregion

            #region 曲线图
            List<TradeChart> tradeCharts = new List<TradeChart>();
            var tm = (t2 - t1).Days;
            if (tm <= 1)
            {
                var th = (t2 - t1).Hours;
                DateTime ths = DateTime.Parse(t1.ToString("yyyy-MM-dd") + $" {t1.Hour}:00:00");
                for (int m = 0; m < th; m++)
                {
                    DateTime tt1 = ths.AddHours(m);
                    DateTime tt2 = ths.AddHours(m + 1);

                    TradeChart tradeChart = new TradeChart();
                    var rc11 = (from c in r1 where c.AddTime >= tt1 && c.AddTime <= tt2 select c).ToList();
                    var rc21 = (from c in r2 where c.AddTime >= tt1 && c.AddTime <= tt2 select c).ToList();
                    var rc31 = (from c in r3 where c.AddTime >= tt1 && c.AddTime <= tt2 select c).ToList();
                    var rc41 = (from c in r4 where c.AddTime >= tt1 && c.AddTime <= tt2 select c).ToList();
                    var bets11 = (from c in betsSource where c.BetTime >= tt1 && c.BetTime <= tt2 select c).ToList();
                    var bets21 = (from c in betsSource where c.BetTime >= tt1 && c.BetTime <= tt2 select c).ToList();
                    var feeOrders11 = (from c in feeOrders1Source where c.AddTime >= tt1 && c.AddTime <= tt2 select c).ToList();
                    
                    tradeChart.x = convert_time_int_to10(tt1);
                    tradeChart.y1 =
                        rc11.Sum(c => c.CoinNumber) - rc21.Sum(c => c.CoinNumber) - rc31.Sum(c => c.CoinNumber) - rc41.Sum(c => c.CoinNumber);
                    tradeChart.y2 = rc11.Sum(c => c.CoinNumber);
                    tradeChart.y3 = rc21.Sum(c => c.CoinNumber);
                    tradeChart.y4 = rc31.Sum(c => c.CoinNumber);
                    tradeChart.y5 = rc41.Sum(c => c.CoinNumber);
                    tradeChart.y6 = feeOrders11.Sum(c => c.FeeAmount); 
                    tradeChart.y7 = bets11.Sum(c => c.BetCoin);
                    tradeChart.y8 = bets21.Count;

                    tradeCharts.Add(tradeChart);
                }
            }
            else
            {
                var td = (t2 - t1).Days;
                DateTime tds = DateTime.Parse(t1.ToString("yyyy-MM-dd" + " 00:00:00"));
                for (int m = 0; m < td; m++)
                {
                    DateTime tt1 = tds.AddDays(m);
                    DateTime tt2 = tds.AddDays(m + 1);

                    TradeChart tradeChart = new TradeChart();
                    var rc11 = (from c in r1 where c.AddTime >= tt1 && c.AddTime <= tt2 select c).ToList();
                    var rc21 = (from c in r2 where c.AddTime >= tt1 && c.AddTime <= tt2 select c).ToList();
                    var rc31 = (from c in r3 where c.AddTime >= tt1 && c.AddTime <= tt2 select c).ToList();
                    var rc41 = (from c in r4 where c.AddTime >= tt1 && c.AddTime <= tt2 select c).ToList();
                    var bets11 = (from c in betsSource where c.BetTime >= tt1 && c.BetTime <= tt2 select c).ToList();
                    var bets21 = (from c in betsSource where c.BetTime >= tt1 && c.BetTime <= tt2 select c).ToList();
                    var feeOrders11 = (from c in feeOrders1Source where c.AddTime >= tt1 && c.AddTime <= tt2 select c).ToList();
                    
                    tradeChart.x = convert_time_int_to10(tt1);
                    tradeChart.y1 =
                        rc11.Sum(c => c.CoinNumber) - rc21.Sum(c => c.CoinNumber) - rc31.Sum(c => c.CoinNumber) - rc41.Sum(c => c.CoinNumber);
                    tradeChart.y2 = rc11.Sum(c => c.CoinNumber);
                    tradeChart.y3 = rc21.Sum(c => c.CoinNumber);
                    tradeChart.y4 = rc31.Sum(c => c.CoinNumber);
                    tradeChart.y5 = rc41.Sum(c => c.CoinNumber);
                    tradeChart.y6 = feeOrders11.Sum(c => c.FeeAmount); 
                    tradeChart.y7 = bets11.Sum(c => c.BetCoin);
                    tradeChart.y8 = bets21.Count;

                    tradeCharts.Add(tradeChart);
                }
            }
            tradeStatis.tradeCharts = tradeCharts;
            #endregion

            info.code = 200;
            info.msg = "success";
            info.data = tradeStatis;

            return info;
        }

        /// <summary>
        /// 获取交易统计排名
        /// </summary>
        /// <param name="OpenId"></param>
        /// <param name="UserType"></param>
        /// <param name="AgentPassportId">代理passportid</param>
        /// <param name="AddTime1"></param>
        /// <param name="AddTime2"></param>
        /// <param name="PageIndex"></param>
        /// <param name="PageSize"></param>
        /// <returns></returns>
        [HttpGet]
        public MsgInfo<List<GameRank>> GetTradeDataRank(string OpenId, string UserType, string AgentPassportId, string AddTime1, string AddTime2, int PageIndex, int PageSize = 20)
        {
            MsgInfo<List<GameRank>> info = new();
            DateTime t = DateTime.Now;
            long _PassportId = IsLogin(OpenId, UserType);
            if (_PassportId <= 0)
            {
                info.code = 400;
                info.msg = "Please login";
                return info;
            }
            if (!string.IsNullOrWhiteSpace(AgentPassportId))
            {
                UserType = "1";
                long.TryParse(AgentPassportId, out _PassportId);
            }

            //t1
            DateTime t1 = DateTime.Parse(DateTime.Now.ToString("yyyy-MM-dd") + $" 00:00:00");
            if (!string.IsNullOrWhiteSpace(AddTime1))
            {
                DateTime.TryParse(AddTime1 + $" 00:00:00", out t1);
            }
            DateTime t2 = DateTime.Parse(DateTime.Now.ToString("yyyy-MM-dd") + $" 23:59:59");
            if (!string.IsNullOrWhiteSpace(AddTime2))
            {
                DateTime.TryParse(AddTime2 + $" 23:59:59", out t2);
            }

            #region 排名
            List<GameRank> gameRanks = new List<GameRank>();
            List<RechargeCashOrder> rechargeCashOrders = db.Queryable<RechargeCashOrder>().Where(c => c.ManageUserPassportId == _PassportId).ToList();
            int no = 0;
            int _RoleId = GetRoleId(_PassportId, UserType);
            List<UserInfo> userInfos = db.Queryable<UserInfo>().Where(c => 
            //c.ParentId == 999999999 && 
            c.ManageUserPassportId == _PassportId && c.RoleId == 4).ToList();
            if (_RoleId == 1)
            {
                rechargeCashOrders = db.Queryable<RechargeCashOrder>().ToList();
                userInfos = db.Queryable<UserInfo>().Where(c => c.ParentId == 999999999 && c.RoleId == 4).ToList();
            }
            //if (!string.IsNullOrWhiteSpace(AddTime1) && !string.IsNullOrWhiteSpace(AddTime2))
            //{
            //    rechargeCashOrders = (from c in rechargeCashOrders where c.AddTime >= t1 && c.AddTime <= t2 select c).ToList();
            //}
            //else if (!string.IsNullOrWhiteSpace(AddTime1))
            //{
            //    rechargeCashOrders = (from c in rechargeCashOrders where c.AddTime >= t1 select c).ToList();
            //}
            //else if (!string.IsNullOrWhiteSpace(AddTime2))
            //{
            //    rechargeCashOrders = (from c in rechargeCashOrders where c.AddTime <= t2 select c).ToList();
            //}
            //List<long> passportIds = (from c in rechargeCashOrders group c by c.PassportId into g select g.Key).ToList();
            //userInfos = (from c in userInfos where passportIds.Contains(c.PassportId) select c).ToList();

            info.data_count = userInfos.Count;
            if (PageIndex > 0)
            {
                userInfos = (from c in userInfos orderby c.AddTime descending select c).Skip((PageIndex - 1) * PageSize).Take(PageSize).ToList();
            }
            foreach (var item in userInfos)
            {
                //手续费类型（0充值，1提款，2优惠红利，3反水派发，4代理佣金，5代理手续费 ）
                //损益 =（收入 - 支出）
                //收入 = 存款
                //支出 = 取款 + 优惠红利 + 流水返佣
                no++;
                var current_passportid = item.PassportId;
                List<UserInfo> users = GetMyChildsAllT<UserInfo>("UserInfo", "", current_passportid, "1");
                List<RechargeCashOrder> rc = GetMyChildsRCOrder<RechargeCashOrder>(current_passportid, "", "1");
                if (!string.IsNullOrWhiteSpace(AddTime1) && !string.IsNullOrWhiteSpace(AddTime2))
                {
                    rc = (from c in rc where c.AddTime >= t1 && c.AddTime <= t2 select c).ToList();
                }
                else if (!string.IsNullOrWhiteSpace(AddTime1))
                {
                    rc = (from c in rc where c.AddTime >= t1 select c).ToList();
                }
                else if (!string.IsNullOrWhiteSpace(AddTime2))
                {
                    rc = (from c in rc where c.AddTime <= t2 select c).ToList();
                }
                List<RechargeCashOrder> rc1 = (from c in rc where c.RCType == 0 select c).ToList();
                List<RechargeCashOrder> rc2 = (from c in rc where c.RCType == 1 select c).ToList();
                List<RechargeCashOrder> rc3 = (from c in rc where c.RCType == 2 select c).ToList();
                List<RechargeCashOrder> rc4 = (from c in rc where c.RCType == 3 select c).ToList();

                GameRank gameRank = new GameRank();
                gameRank.Rank = no;
                gameRank.UserName = item.UserName;
                gameRank.MyChildCount = users.Count.ToString();
                gameRank.MyChildWinLose = (rc1.Sum(c => c.CoinNumber) - rc2.Sum(c => c.CoinNumber) - rc3.Sum(c => c.CoinNumber) - rc4.Sum(c => c.CoinNumber)).ToString();

                gameRanks.Add(gameRank);
            }
            #endregion

            info.code = 200;
            info.msg = "success";
            info.data = gameRanks;
            return info;
        }

        /// <summary>
        /// 获取用户统计(UserType:0为管理员用户，1为会员用户)
        /// </summary>
        /// <param name="OpenId"></param>
        /// <param name="UserType"></param>
        /// <param name="AgentPassportId">代理PassportId</param>
        /// <param name="AddTime1"></param>
        /// <param name="AddTime2"></param>
        /// <returns></returns>
        [HttpGet]
        public MsgInfo<RetuUserStatis> GetUserDataStatis(string OpenId, string UserType, string AgentPassportId, string AddTime1, string AddTime2)
        {
            MsgInfo<RetuUserStatis> info = new();
            DateTime t = DateTime.Now;
            long _PassportId = IsLogin(OpenId,UserType);
            if (_PassportId <= 0)
            {
                info.code = 400;
                info.msg = "Please login";
                return info;
            }
            if (!string.IsNullOrWhiteSpace(AgentPassportId))
            {
                UserType = "1";
                long.TryParse(AgentPassportId, out _PassportId);
            }
            RetuUserStatis userStatis = new RetuUserStatis();
            DateTime t1 = DateTime.Parse(DateTime.Now.ToString("yyyy-MM-dd") + $" 00:00:00");
            if (!string.IsNullOrWhiteSpace(AddTime1))
            {
                DateTime.TryParse(AddTime1 + $" 00:00:00", out t1);
            }
            DateTime t2 = DateTime.Parse(DateTime.Now.ToString("yyyy-MM-dd") + $" 23:59:59");
            if (!string.IsNullOrWhiteSpace(AddTime2))
            {
                DateTime.TryParse(AddTime2 + $" 23:59:59", out t2);
            }
            //充值提现(0充值 1提现)
            //手续费类型（0游戏 1返佣 2红利 ）
            List<UserInfo> userInfos = GetMyChilds<UserInfo>(_PassportId, "",  UserType);//新增会员/累计会员
            List<UserInfo> agentInfos = GetMyChilds<UserInfo>(_PassportId, " and b.RoleId=4 ", UserType); //新增代理/累计代理
            List<long> gameUsers = userInfos.Select(c => c.PassportId).ToList();
            List<long> gameAgent = agentInfos.Select(c => c.PassportId).ToList();
            var bets1 = db.Queryable<Bet>().Where(c => gameUsers.Contains(c.PassportId)).ToList();//会员游戏/会员活跃数量
            var bets2 = db.Queryable<Bet>().Where(c => gameAgent.Contains(c.PassportId)).ToList();//会员代理/代理活跃数量

            List<UserInfo> userInfosTB = new List<UserInfo>();
            List<UserInfo> agentInfosTB = new List<UserInfo>();
            var bets1TB = new List<Bet>();
            var bets2TB = new List<Bet>();

            //时间
            if (!string.IsNullOrWhiteSpace(AddTime1) && !string.IsNullOrWhiteSpace(AddTime2))
            {
                DateTime at1 = DateTime.Now;
                DateTime.TryParse(AddTime1 + $" 00:00:00", out at1);
                DateTime at2 = DateTime.Now;
                DateTime.TryParse(AddTime2 + $" 00:00:00", out at2);
                double TM = (at2.AddDays(1) - at1).TotalMinutes;
                DateTime t3 = t1.AddMinutes(-TM);

                userInfos = (from c in userInfos where c.AddTime >= t1 && c.AddTime <= t2 select c).ToList();
                agentInfos = (from c in agentInfos where c.AddTime >= t1 && c.AddTime <= t2 select c).ToList();
                bets1 = (from c in bets1 where c.AddTime >= t1 && c.AddTime <= t2 select c).ToList();
                bets2 = (from c in bets2 where c.AddTime >= t1 && c.AddTime <= t2 select c).ToList();

                userInfosTB = (from c in userInfos where c.AddTime >= t3 && c.AddTime <= t1 select c).ToList();
                agentInfosTB = (from c in agentInfos where c.AddTime >= t3 && c.AddTime <= t1 select c).ToList();
                bets1TB = (from c in bets1 where c.AddTime >= t3 && c.AddTime <= t1 select c).ToList();
                bets2TB = (from c in bets2 where c.AddTime >= t3 && c.AddTime <= t1 select c).ToList();
            }
            else if (!string.IsNullOrWhiteSpace(AddTime1))
            {
                userInfos = (from c in userInfos where c.AddTime >= t1 select c).ToList();
                agentInfos = (from c in agentInfos where c.AddTime >= t1 select c).ToList();
                bets1 = (from c in bets1 where c.AddTime >= t1 select c).ToList();
                bets2 = (from c in bets2 where c.AddTime >= t1 select c).ToList();
            }
            else if (!string.IsNullOrWhiteSpace(AddTime2))
            {
                userInfos = (from c in userInfos where c.AddTime <= t2 select c).ToList();
                agentInfos = (from c in agentInfos where c.AddTime <= t2 select c).ToList();
                bets1 = (from c in bets1 where c.AddTime <= t2 select c).ToList();
                bets2 = (from c in bets2 where c.AddTime <= t2 select c).ToList();
            }
            bets1 = (from c in bets1 group c by c.PassportId into g select new Bet { PassportId = g.Key }).ToList();
            bets2 = (from c in bets2 group c by c.PassportId into g select new Bet { PassportId = g.Key }).ToList();

            userStatis.NewUserCount = userInfos.Count;
            userStatis.NewAgentCount = agentInfos.Count;
            userStatis.AllUserCount = userInfos.Count;
            userStatis.AllAgentCount = agentInfos.Count;
            userStatis.AllGameUserCount = bets1.Count;
            userStatis.AllGameAgentCount = bets2.Count;
            userStatis.MemberActiveCount = bets1.Count;
            userStatis.AgentActiveCount = bets2.Count;

            if (!string.IsNullOrWhiteSpace(AddTime1) && !string.IsNullOrWhiteSpace(AddTime2))
            {
                var activeTB = (from c in bets1TB group c by c.PassportId into g select g.Key).ToList();
                var NewUserCountTB = userInfosTB.Count;
                var NewAgentCountTB = agentInfosTB.Count;
                var AllGameUserCountTB = bets1TB.Count;
                var AllGameAgentCountTB = bets2TB.Count;
                var MemberActiveCountTB = db.Queryable<UserInfo>().Where(c => activeTB.Contains(c.PassportId) && c.RoleId == 100).ToList().Count;
                var AgentActiveCountTB = db.Queryable<UserInfo>().Where(c => activeTB.Contains(c.PassportId) && c.RoleId == 4).ToList().Count;

                userStatis.NewUserCountTB = NewUserCountTB == 0 ? "0%" : ((userStatis.NewUserCount - NewUserCountTB) / NewUserCountTB * 100).ToString("F2") + "%";
                userStatis.NewAgentCountTB = NewAgentCountTB == 0 ? "0%" : ((userStatis.NewAgentCount - NewAgentCountTB) / NewAgentCountTB * 100).ToString("F2") + "%";
                userStatis.AllGameUserCountTB = AllGameUserCountTB == 0 ? "0%" : ((userStatis.AllGameUserCount - AllGameUserCountTB) / AllGameUserCountTB * 100).ToString("F2") + "%";
                userStatis.AllGameAgentCountTB = AllGameAgentCountTB == 0 ? "0%" : ((userStatis.AllGameAgentCount - AllGameAgentCountTB) / AllGameAgentCountTB * 100).ToString("F2") + "%";
                userStatis.MemberActiveCountTB = MemberActiveCountTB == 0 ? "0%" : ((userStatis.MemberActiveCount - MemberActiveCountTB) / MemberActiveCountTB * 100).ToString("F2") + "%";
                userStatis.AgentActiveCountTB = AgentActiveCountTB == 0 ? "0%" : ((userStatis.AgentActiveCount - AgentActiveCountTB) / AgentActiveCountTB * 100).ToString("F2") + "%";
            }

            #region 曲线图
            List<TradeChart> tradeCharts = new List<TradeChart>();
            var tm = (t2 - t1).Days;
            if (tm <= 1)
            {
                var th = (t2 - t1).Hours;
                DateTime ths = DateTime.Parse(t1.ToString("yyyy-MM-dd") + $" {t1.Hour}:00:00");
                for (int m = 0; m < th; m++)
                {
                    DateTime tt1 = ths.AddHours(m);
                    DateTime tt2 = ths.AddHours(m + 1);

                    TradeChart tradeChart = new TradeChart();
                    var userInfos1 = (from c in userInfos where c.AddTime >= tt1 && c.AddTime <= tt2 select c).ToList();
                    var agentInfos1 = (from c in agentInfos where c.AddTime >= tt1 && c.AddTime <= tt2 select c).ToList();
                    var bets11 = (from c in bets1 where c.AddTime >= tt1 && c.AddTime <= tt2 select c).ToList();
                    var bets21 = (from c in bets2 where c.AddTime >= tt1 && c.AddTime <= tt2 select c).ToList();
                    bets11 = (from c in bets11 group c by c.PassportId into g select new Bet { PassportId = g.Key }).ToList();
                    bets21 = (from c in bets21 group c by c.PassportId into g select new Bet { PassportId = g.Key }).ToList();

                    tradeChart.x = convert_time_int_to10(tt1);
                    tradeChart.y1 = userInfos1.Count;
                    tradeChart.y2 = agentInfos1.Count;
                    tradeChart.y3 = userInfos1.Count;
                    tradeChart.y4 = agentInfos1.Count;
                    tradeChart.y5 = bets11.Count;
                    tradeChart.y6 = bets21.Count;
                    tradeChart.y7 = bets11.Count;
                    tradeChart.y8 = bets21.Count;

                    tradeCharts.Add(tradeChart);
                }
            }
            else
            {
                var td = (t2 - t1).Days;
                DateTime tds = DateTime.Parse(t1.ToString("yyyy-MM-dd" + " 00:00:00"));
                for (int m = 0; m < td; m++)
                {
                    DateTime tt1 = tds.AddDays(m);
                    DateTime tt2 = tds.AddDays(m + 1);

                    TradeChart tradeChart = new TradeChart();
                    var userInfos1 = (from c in userInfos where c.AddTime >= tt1 && c.AddTime <= tt2 select c).ToList();
                    var agentInfos1 = (from c in agentInfos where c.AddTime >= tt1 && c.AddTime <= tt2 select c).ToList();
                    var bets11 = (from c in bets1 where c.AddTime >= tt1 && c.AddTime <= tt2 select c).ToList();
                    var bets21 = (from c in bets2 where c.AddTime >= tt1 && c.AddTime <= tt2 select c).ToList();
                    bets11 = (from c in bets11 group c by c.PassportId into g select new Bet { PassportId = g.Key }).ToList();
                    bets21 = (from c in bets21 group c by c.PassportId into g select new Bet { PassportId = g.Key }).ToList();

                    tradeChart.x = convert_time_int_to10(tt1);
                    tradeChart.y1 = userInfos1.Count;
                    tradeChart.y2 = agentInfos1.Count;
                    tradeChart.y3 = userInfos1.Count;
                    tradeChart.y4 = agentInfos1.Count;
                    tradeChart.y5 = bets11.Count;
                    tradeChart.y6 = bets21.Count;
                    tradeChart.y7 = bets11.Count;
                    tradeChart.y8 = bets21.Count;

                    tradeCharts.Add(tradeChart);
                }
            }
            userStatis.tradeCharts = tradeCharts;
            #endregion

            info.code = 200;
            info.msg = "success";
            info.data = userStatis;

            return info;
        }

        #endregion

        #region 运营设置
        /// <summary>
        /// 获取运营设置
        /// </summary>
        /// <param name="OpenId"></param>
        /// <returns></returns>
        [HttpGet]
        public MsgInfo<OperateSys> GetOperate(string OpenId)
        {
            MsgInfo<OperateSys> info = new();
            long _PassportId = IsLogin(OpenId);
            if (_PassportId <= 0)
            {
                info.code = 400;
                info.msg = "Please login";
                return info;
            }
            OperateSys operate = db.Queryable<OperateSys>().First(c => c.PassportId == _PassportId);

            info.code = 200;
            info.msg = "success";
            info.data = operate;
            return info;
        }

        /// <summary>
        /// 添加/编辑运营设置
        /// </summary>
        /// <param name="param">
        /// {"OpenId":"9e26f6e2244f7e34ed8b4eabdcb2f9c4","ShopDomain":"商户域名","AgentDomain":"代理域名","AgentType":"默认代理类型",
        /// "AreaTime":"运营地区","GoogleVaild":"google验证码"}
        /// </param>
        /// <returns></returns>
        [HttpPost]
        public MsgInfo<string> PostOperate(dynamic param)
        {
            MsgInfo<string> info = new();
            DateTime t = DateTime.Now;
            var data = JsonConvert.DeserializeObject<dynamic>(param.ToString());
            string OpenId = data.OpenId.ToString();
            if (Object.ReferenceEquals(null, data))
            {
                info.code = 400;
                info.msg = "Network Exception";
                return info;
            }
            long _PassportId = IsLogin(OpenId);
            if (_PassportId <= 0)
            {
                info.code = 400;
                info.msg = "Please login";
                return info;
            }

            string ShopDomain = data.ShopDomain.ToString();
            string AgentDomain = data.AgentDomain.ToString();
            string AgentType = data.AgentType.ToString();
            string AreaTime = data.AreaTime.ToString();
            string GoogleVaild = data.GoogleVaild.ToString();

            int _AgentType = 1;
            int.TryParse(AgentType, out _AgentType);
            int _GoogleVaild = 1;
            int.TryParse(GoogleVaild, out _GoogleVaild);
            DateTime _AreaTime = DateTime.Now;
            DateTime.TryParse(AreaTime, out _AreaTime);

            OperateSys sys1 = db.Queryable<OperateSys>().First(c => (c.ShopDomain.ToUpper().Trim() == ShopDomain.ToUpper().Trim() ||
            c.AgentDomain.ToUpper().Trim() == AgentDomain.ToUpper().Trim()) && c.PassportId != _PassportId);
            if (sys1 != null)
            {
                info.code = 400;
                info.msg = "Domain already exists";
                return info;
            }

            OperateSys operate = db.Queryable<OperateSys>().First(c => c.PassportId == _PassportId);
            //添加
            if (operate == null)
            {
                OperateSys sys = new()
                {
                    AddTime = t,
                    UpdateTime = t,
                    IsValid = 1,
                    Sort = 1,
                    Ip = YRHelper.GetClientIPAddress(HttpContext),

                    ShopDomain = ShopDomain,
                    AgentDomain = AgentDomain,
                    AgentType = _AgentType,
                    AreaTime = _AreaTime,
                    GoogleVaild = _GoogleVaild,
                    PassportId = _PassportId
                };
                Dictionary<string, object> keyValues = new Dictionary<string, object>();
                keyValues.Add("IsGoogle", _GoogleVaild);
                keyValues.Add("UpdateTime", DateTime.Now);
                keyValues.Add("Id", db.Queryable<AdminUser>().First(x => x.OpenId == OpenId).Id);
                db.Updateable(keyValues).AS("AdminUser").WhereColumns("Id").ExecuteCommand();
                int m = db.Insertable(sys).ExecuteCommand();
                if (m < 1)
                {
                    info.code = 400;
                    info.msg = "Network Exception";
                    return info;
                }
            }
            else
            {
                int m = db.Updateable<OperateSys>().SetColumns(c => c.UpdateTime == t).
                    SetColumns(c => c.ShopDomain == ShopDomain).
                    SetColumns(c => c.AgentDomain == AgentDomain).
                    SetColumns(c => c.AgentType == _AgentType).
                    SetColumns(c => c.AreaTime == _AreaTime).
                    SetColumns(c => c.GoogleVaild == _GoogleVaild).
                    Where(c => c.PassportId == _PassportId).ExecuteCommand();
                if (m < 1)
                {
                    info.code = 400;
                    info.msg = "Network Exception";
                    return info;
                }
            }
            info.code = 200;
            info.msg = "success";

            return info;
        }
        #endregion


        /// <summary>
        /// 生成token
        /// </summary>
        /// <returns></returns>
        private string CreateOpenId()
        {
            string token = YRHelper.GetRandom(20);
            token = YRHelper.OpenIdEncrypt(token);
            return token;
        }

        /// <summary>
        /// 生成推荐码
        /// </summary>
        /// <returns></returns>
        private string CreateTJCode()
        {
            string TJCode = YRHelper.GetRandom(8);
            return TJCode;
        }

        /// <summary>
        /// 判断是否登录(UserType:0为管理员用户，1为会员用户)
        /// </summary>
        /// <param name="OpenId">OpenId</param>
        /// <param name="UserType">用户类型</param>
        /// <returns></returns>
        private Int64 IsLogin(string OpenId, string UserType = "0")
        {
            Int64 PassportId = 0;
            if (UserType == "0")
            {
                AdminUser record = db.Queryable<AdminUser>().First(c => c.OpenId == OpenId);
                if (record != null)
                {
                    PassportId = record.PassportId;
                    //LoginExpirationDuration
                    Sys sys = db.Queryable<Sys>().First(c => c.Keys == "LoginExpirationDuration");
                    if (sys == null)
                    {
                        PassportId = -1;
                    }
                    double Durationt = Convert.ToDouble(sys.Value);
                    DateTime t = DateTime.Now;
                    double TM = (t - record.UpdateTime).TotalMinutes;
                    LogHelper.Debug($"[IsLogin],PassportId:{PassportId}|LoginTime:{t}|PrevTime:{record.UpdateTime}|TM:{TM}|Durationt:{Durationt}");
                    if (TM > Durationt)
                    {
                        PassportId = -1;
                    }
                }
            }
            else
            {
                UserInfo record = db.Queryable<UserInfo>().First(c => c.OpenId == OpenId);
                if (record != null)
                {
                    PassportId = record.PassportId;
                    //LoginExpirationDuration
                    Sys sys = db.Queryable<Sys>().First(c => c.Keys == "LoginExpirationDuration");
                    if (sys == null)
                    {
                        PassportId = -1;
                    }
                    double Durationt = Convert.ToDouble(sys.Value);
                    DateTime t = DateTime.Now;
                    double TM = (t - record.UpdateTime).TotalMinutes;
                    LogHelper.Debug($"[IsLogin],PassportId:{PassportId}|LoginTime:{t}|PrevTime:{record.UpdateTime}|TM:{TM}|Durationt:{Durationt}");
                    if (TM > Durationt)
                    {
                        PassportId = -1;
                    }
                }
            }

            return PassportId;
        }

        /// <summary>
        /// 获取角色ID
        /// </summary>
        /// <param name="PassportId"></param>
        /// <param name="UserType"></param>
        /// <returns></returns>
        private int GetRoleId(long PassportId, string UserType = "0")
        {
            int RoleId = 0;
            if (UserType == "0")
            {
                AdminUser record = db.Queryable<AdminUser>().First(c => c.PassportId == PassportId);
                if (record != null)
                {
                    RoleId = record.RoleId;
                }
            }
            else
            {
                UserInfo record = db.Queryable<UserInfo>().First(c => c.PassportId == PassportId);
                if (record != null)
                {
                    RoleId = record.RoleId;
                }
            }

            return RoleId;
        }

        /// <summary>
        /// 根据商户号获取支付信息
        /// </summary>
        /// <param name="_merchant_no">商户号</param>
        /// <returns></returns>
        private PaySys GetPayMsg(string _merchant_no)
        {
            PaySys pay = null;
            if (string.IsNullOrWhiteSpace(_merchant_no))
            {
                return pay;
            }
            if (redis.Exists($"pay_{_merchant_no}"))
            {
                if (redis.Get<PaySys>($"pay_{_merchant_no}") == null)
                {
                    pay = db.Queryable<PaySys>().First(c => c.merchant_no == _merchant_no);
                    redis.Set($"pay_{_merchant_no}", pay, 60 * 60 * 24);
                }
            }
            else
            {
                pay = db.Queryable<PaySys>().First(c => c.merchant_no == _merchant_no);
                redis.Set($"pay_{_merchant_no}", pay, 60 * 60 * 24);
            }
            pay = redis.Get<PaySys>($"pay_{_merchant_no}");
            return pay;
        }

        /// <summary>
        /// 是否中奖
        /// </summary>
        /// <param name="GameDetailsId">玩法id</param>
        /// <param name="BetCoin">下注金额</param>
        /// <param name="hash_no">哈希值</param>
        /// <returns></returns>
        [HttpGet]
        private bool IsWin(int GameDetailsId, decimal GameFee, decimal InvalidGameFee, decimal MinQuota, decimal MaxQuota, decimal BetCoin, string hash_no, decimal Odds,
            decimal Odds0, decimal Odds1, decimal Odds2, decimal Odds3, decimal Odds4, decimal Odds5,
            decimal Odds6, decimal Odds7, decimal Odds8, decimal Odds9, out decimal BetWinCoin, out string OpenResult)
        {
            bool flag = false;
            int _BetCoin = decimal.ToInt32(BetCoin);
            OpenResult = string.Empty;

            {
                if (BetCoin < MinQuota || BetCoin > MaxQuota)
                {
                    BetWinCoin = Convert.ToDecimal((BetCoin - (BetCoin * InvalidGameFee / 100)).ToString("F6"));
                    OpenResult = "无效投注";
                    return false;
                }
            }
            BetWinCoin = Convert.ToDecimal((Odds * _BetCoin * (1 - GameFee / 100)).ToString("F6"));

            switch (GameDetailsId)
            {
                case 1:
                    {
                        var hashno = hash_no.Substring(hash_no.Length - 3);
                        OpenResult = "全字母";
                        //【区块哈希值】后三位的最后数字
                        //玩家转账金额个位数与开奖结果单或双一致，则玩家中奖
                        //如不一致或哈希值后三位全为字母则玩家未中奖
                        foreach (var item in hashno.Reverse())
                        {
                            int value = -1;
                            if (int.TryParse(item.ToString(), out value))
                            {
                                if (value % 2 == 0)
                                {
                                    OpenResult = "双";
                                }
                                else
                                {
                                    OpenResult = "单";
                                }
                                if (value % 2 == _BetCoin % 2)
                                {
                                    return true;
                                }
                                else
                                {
                                    BetWinCoin = 0;
                                    return false;
                                }
                            }
                        }
                    }
                    break;
                case 2:
                    {
                        var hashno = hash_no.Substring(hash_no.Length - 2);
                        OpenResult = string.Empty;
                        //【区块哈希值】后两位
                        //【数字+字母】或【字母+数字】，则玩家中奖
                        //【数字+数字】或【字母+字母】，则玩家不中奖
                        int temp = 0;
                        foreach (var item in hashno.Reverse())
                        {
                            int value = -1;
                            if (int.TryParse(item.ToString(), out value))
                            {
                                OpenResult += "数字+";
                                temp++;
                            }
                            else
                            {
                                OpenResult += "字母+";
                            }
                        }
                        OpenResult = OpenResult.Trim('+');
                        if (temp == 1)
                        {
                            return true;
                        }
                    }
                    break;
                case 3:
                    {
                        var hashno = hash_no.Substring(hash_no.Length - 3);
                        OpenResult = "全字母";
                        if (_BetCoin > 9)
                        {
                            int.TryParse(_BetCoin.ToString().Substring(_BetCoin.ToString().Length - 1), out _BetCoin);
                        }
                        //以所在区块的区块哈希值(Block Hash)从右到左的第一个数字为依据
                        foreach (var item in hashno.Reverse())
                        {
                            int value = -1;
                            if (int.TryParse(item.ToString(), out value))
                            {
                                if (value >= 5)
                                {
                                    OpenResult = "大";
                                }
                                else
                                {
                                    OpenResult = "小";
                                }

                                if ((value >= 5 && _BetCoin >= 5) || (value < 5 && _BetCoin < 5))
                                {
                                    return true;
                                }
                                else
                                {
                                    BetWinCoin = 0;
                                    return false;
                                }
                            }
                        }
                    }
                    break;
                case 4:
                    {
                        int int_zuang = 0;
                        int int_xian = 0;
                        int temp = 0;
                        decimal.TryParse(Convert.ToDecimal(BetCoin).ToString("F1"), out BetCoin);
                        int zx_type = Convert.ToInt32(BetCoin.ToString().Substring(BetCoin.ToString().IndexOf(".") + 1));
                        var hashno = hash_no.Substring(hash_no.Length - 5);
                        OpenResult = "无效投注";
                        BetWinCoin = Convert.ToDecimal((Odds * BetCoin * (1 - GameFee / 100)).ToString("F6"));
                        for (int m = 0; m < hashno.Length; m++)
                        {
                            if (m < 2)
                            {
                                if (int.TryParse(hashno[m].ToString(), out temp))
                                {
                                    int_zuang += temp;
                                    if (int_zuang >= 10)
                                    {
                                        int_zuang -= 10;
                                    }
                                }
                            }
                            if (m > 2)
                            {
                                if (int.TryParse(hashno[m].ToString(), out temp))
                                {
                                    int_xian += temp;
                                    if (int_xian >= 10)
                                    {
                                        int_xian -= 10;
                                    }
                                }
                            }
                        }

                        switch (zx_type)
                        {
                            case 1:

                                //庄
                                if (int_zuang > int_xian)
                                {
                                    OpenResult = "庄";
                                    return true;
                                }
                                else if (int_zuang < int_xian)
                                {
                                    OpenResult = "闲";
                                }
                                else if (int_zuang == int_xian)
                                {
                                    BetWinCoin = Convert.ToDecimal((BetCoin - (BetCoin * InvalidGameFee / 100)).ToString("F6"));
                                    OpenResult = "和";
                                    return false;
                                }
                                break;
                            case 2:
                                //闲
                                if (int_xian > int_zuang)
                                {
                                    OpenResult = "闲";
                                    return true;
                                }
                                else if (int_xian < int_zuang)
                                {
                                    OpenResult = "庄";
                                }
                                else if (int_zuang == int_xian)
                                {
                                    BetWinCoin = Convert.ToDecimal((BetCoin - (BetCoin * InvalidGameFee / 100)).ToString("F6"));
                                    OpenResult = "和";
                                    return false;
                                }
                                break;
                            case 3:
                                //和
                                if (int_xian == int_zuang)
                                {
                                    BetWinCoin = Convert.ToDecimal((Odds0 * BetCoin * (1 - GameFee / 100)).ToString("F6"));
                                    OpenResult = "和";
                                    return true;
                                }
                                else if (int_xian < int_zuang)
                                {
                                    OpenResult = "庄";
                                }
                                else if (int_xian > int_zuang)
                                {
                                    OpenResult = "闲";
                                }
                                break;
                            default:
                                BetWinCoin = Convert.ToDecimal((BetCoin - (BetCoin * InvalidGameFee / 100)).ToString("F6"));
                                return false;
                        }
                    }
                    break;
                case 5:
                    {
                        List<SANGONG> sglist = new List<SANGONG>();
                        sglist.Add(new()
                        {
                            odds = 1,
                            odds_field = Odds1
                        });
                        sglist.Add(new()
                        {
                            odds = 2,
                            odds_field = Odds2
                        });
                        sglist.Add(new()
                        {
                            odds = 3,
                            odds_field = Odds3
                        });
                        sglist.Add(new()
                        {
                            odds = 4,
                            odds_field = Odds4
                        });
                        sglist.Add(new()
                        {
                            odds = 5,
                            odds_field = Odds5
                        });
                        sglist.Add(new()
                        {
                            odds = 6,
                            odds_field = Odds6
                        });
                        sglist.Add(new()
                        {
                            odds = 7,
                            odds_field = Odds7
                        });
                        sglist.Add(new()
                        {
                            odds = 8,
                            odds_field = Odds8
                        });
                        sglist.Add(new()
                        {
                            odds = 9,
                            odds_field = Odds9
                        });
                        sglist.Add(new()
                        {
                            odds = 0,
                            odds_field = Odds0
                        });

                        int int_zuang = 0;
                        int int_xian = 0;
                        var hashno_z = hash_no.Substring(hash_no.Length - 5, 3);
                        var hashno_x = hash_no.Substring(hash_no.Length - 3);
                        OpenResult = string.Empty;
                        _BetCoin = _BetCoin / 10;
                        //庄家
                        foreach (var item in hashno_z)
                        {
                            int value = -1;
                            if (int.TryParse(item.ToString(), out value))
                            {
                                int_zuang += value;
                            }
                        }
                        //玩家
                        foreach (var item in hashno_x)
                        {
                            int value = -1;
                            if (int.TryParse(item.ToString(), out value))
                            {
                                int_xian += value;
                            }
                        }
                        int_zuang = int_zuang % 10;
                        int_xian = int_xian % 10;
                        if (int_zuang == 0)
                        {
                            int_zuang = 10;
                        }
                        if (int_xian == 0)
                        {
                            int_xian = 10;
                        }
                        if (int_zuang == int_xian)//和
                        {
                            if (int_zuang == 1 || int_zuang == 2 || int_zuang == 3)
                            {
                                var current_odds = (from c in sglist where c.odds == int_zuang select c.odds_field).FirstOrDefault();
                                BetWinCoin = Convert.ToDecimal((BetCoin - _BetCoin * current_odds).ToString("F6"));
                                OpenResult = $"和|庄{int_zuang}|闲{int_xian}";
                                return false;
                            }

                            BetWinCoin = Convert.ToDecimal((BetCoin - _BetCoin * (1 - InvalidGameFee / 100)).ToString("F6"));
                            OpenResult = "和";
                            return false;
                        }
                        else if (int_zuang > int_xian)//玩家输
                        {
                            var current_odds = (from c in sglist where c.odds == int_zuang select c.odds_field).FirstOrDefault();
                            BetWinCoin = Convert.ToDecimal((BetCoin - _BetCoin * current_odds).ToString("F6"));
                            OpenResult = $"和|庄{int_zuang}|闲{int_xian}";
                            return false;
                        }
                        else if (int_zuang < int_xian)//玩家赢
                        {
                            var current_odds = (from c in sglist where c.odds == int_xian select c.odds_field).FirstOrDefault();
                            BetWinCoin = Convert.ToDecimal((BetCoin + _BetCoin * current_odds).ToString("F6"));
                            OpenResult = $"和|庄{int_zuang}|闲{int_xian}";
                            return false;
                        }
                    }
                    break;
            }

            BetWinCoin = 0;
            return flag;
        }

        [HttpPost]
        public MsgInfo<string> test(int GameDetailsId, decimal BetCoin, string hash_no, decimal Odds,
            decimal Odds0, decimal Odds1, decimal Odds2, decimal Odds3, decimal Odds4, decimal Odds5,
            decimal Odds6, decimal Odds7, decimal Odds8, decimal Odds9)
        {
            MsgInfo<string> info = new();
            decimal BetWinCoin = 0;
            string hash_no_out = string.Empty;
            bool flag = IsWin(1,GameDetailsId, 5, 1, 1000, BetCoin, hash_no, Odds,
            Odds0, Odds1, Odds2, Odds3, Odds4, Odds5,
            Odds6, Odds7, Odds8, Odds9, out BetWinCoin, out hash_no_out);
            if (!flag)
            {
                info.code = 400;
                info.msg = BetWinCoin.ToString("F2");
                return info;
            }
            info.code = 200;
            info.msg = BetWinCoin.ToString("F2");

            return info;
        }

        [HttpPost]
        public async Task<MsgInfo<string>> test1(int GameDetailsId)
        {
            MsgInfo<string> info = new();
            var BetWinCoin = Convert.ToDecimal((6 - (6 * 0.5 / 100)).ToString("F2"));
            int m = int.Parse("ww");
            info.code = 200;
            info.msg = "";
            return info;
        }


        /// <summary>
        /// 时间类型转成时间戳(10位)  
        /// </summary>
        /// <param name="time"></param>
        /// <returns></returns>
        private int convert_time_int_to10(DateTime time)
        {
            int intResult = 0;
            System.DateTime startTime = TimeZone.CurrentTimeZone.ToLocalTime(new System.DateTime(1970, 1, 1));
            intResult = (int)(time - startTime).TotalSeconds;
            return intResult;
        }

        /// <summary>
        /// 添加活动方案
        /// </summary>
        /// <param name="rtn"></param>
        /// <returns></returns>
        [HttpPost]
        public MsgInfo<string> IsEventHK(RtnEvenetHK rtn)
        {
            MsgInfo<string> info = new();
            long _PassportId = IsLogin(rtn.OpenId);
            if (rtn.UserNum < rtn.ToDayUserNum || rtn.StaTime > rtn.EndTime)
            {
                info.code = 400;
                info.msg = "Parameter error";
                return info;
            }
            if (_PassportId <= 0)
            {
                info.code = 400;
                info.msg = "Please login";
                return info;
            }

            var my = db.Queryable<AdminUser>().Where(x => x.OpenId == rtn.OpenId).First();

            if (my.IsGoogle==1)
                if (!GoogleHelp.CheckCode(rtn.GoogleCode, my.GoogleKey))
                {
                    info.code = 400;
                    info.msg = "Verification code error";
                    return info;
                }
            
            
            var guid = Guid.NewGuid().ToString();
            var ips = YRHelper.GetClientIPAddress(HttpContext);
            int isvalid = 1;
            int.TryParse(rtn.IsValid, out isvalid);
            var evenethk = new EventHK()
            {
                Ip = ips,
                GUID = guid,
                RulesType = rtn.RulesType,
                GameTypeId = rtn.GameTypeId,
                EventName = rtn.EventName,
                AddTime = rtn.StaTime,
                UpdateTime = rtn.EndTime,
                IsAudit = rtn.IsAudit,
                IsValid = isvalid,
                ManageUserPassportId = my.PassportId,
                ToDayUserNum = rtn.ToDayUserNum,
                 ToDayAllUserNum=rtn.ToDayAllUserNum,
                  AllUserNum=rtn.AllUserNum,
                PassportId = rtn.PassportId,
                UserNum = rtn.UserNum,
                CoinType = rtn.CoinType
            };
            List<EventRules> lis_rules = rtn.EventRules.Select(x => new EventRules
            {
                AddTime = rtn.StaTime,
                UpdateTime = rtn.EndTime,
                BetAmount = x.BetAmount,
                Guid = guid,
                Ip = ips,
                IsValid = isvalid,
                Reward = x.Reward
            }).ToList();
            try
            {
                db.Ado.BeginTran();
                db.Insertable<EventHK>(evenethk).ExecuteCommand();
                db.Insertable<EventRules>(lis_rules).ExecuteCommand();
                db.Ado.CommitTran();
                info.msg = "success";
            }
            catch (Exception)
            {

                db.Ado.RollbackTran();
                info.code = 400;
                info.msg = "Network Exception";
            }
            return info;

        }
        /// <summary>
        /// 修改活动方案
        /// </summary>
        /// <param name="rtn"></param>
        /// <returns></returns>
        [HttpPost]
        public MsgInfo<string> EditEventHK(RtnEvenetHK rtn)
        {
            MsgInfo<string> info = new();
            long _PassportId = IsLogin(rtn.OpenId);
            if (_PassportId <= 0)
            {
                info.code = 400;
                info.msg = "Please login";
                return info;
            }
            if (rtn.UserNum < rtn.ToDayUserNum)
            {
                info.code = 200;
                info.msg = "Parameter error";
                return info;
            }
    
            var my = db.Queryable<AdminUser>().Where(x => x.OpenId == rtn.OpenId).First();

            if (my.IsGoogle == 1)
                if (!GoogleHelp.CheckCode(rtn.GoogleCode, my.GoogleKey))
                {
                    info.code = 400;
                    info.msg = "Verification code error";
                    return info;
                }
           
            var alleve = db.Queryable<EventHK>().First(x => x.ManageUserPassportId == my.PassportId  && x.Id == rtn.Id);
            var ips = YRHelper.GetClientIPAddress(HttpContext);
            int isvalid = 1;
            int.TryParse(rtn.IsValid, out isvalid);
            EventHK hK = new EventHK()
            {
                Id = rtn.Id,
                AddTime = rtn.StaTime,
                CoinType = rtn.CoinType,
                EventName = rtn.EventName,
                GameTypeId = rtn.GameTypeId,
                Ip = ips,
                GUID = alleve.GUID,
                IsAudit = rtn.IsAudit,
                IsValid = isvalid,
                UserNum = rtn.UserNum,
                ManageUserPassportId = my.PassportId,
                PassportId = rtn.PassportId,
                OperationTime = DateTime.Now,
                RulesType = rtn.RulesType,
                ToDayUserNum = rtn.ToDayUserNum,
                UpdateTime = rtn.EndTime,
                 ToDayAllUserNum=rtn.ToDayAllUserNum,
                  AllUserNum=rtn.AllUserNum
            };
            var eventrules = db.Queryable<EventRules>().Where(x => x.Guid == alleve.GUID && x.IsValid == 1).ToList();

            var is_del = new List<EventRules>();
            var is_ins = rtn.EventRules.Where(d => d.Id == 0).Select(d => new EventRules() { AddTime = DateTime.Now, BetAmount = d.BetAmount, Guid = alleve.GUID, IsValid = 1, Ip = ips, Reward = d.Reward, UpdateTime = DateTime.Now }).ToList();

            eventrules.ForEach(x =>
            {

                if (!rtn.EventRules.Where(x => x.Id != 0).Any(d => d.Id == x.Id))
                {
                    x.IsValid = 0;
                    is_del.Add(x);
                }
            });

            //更新
            var edit_rules = (from i in eventrules
                              join o in rtn.EventRules
                              on i.Id equals o.Id
                              select new EventRules()
                              {
                                  Id = i.Id,
                                  AddTime = rtn.StaTime,
                                  BetAmount = o.BetAmount,
                                  Guid = i.Guid,
                                  Ip = ips,
                                  IsValid = i.IsValid,
                                  Reward = o.Reward,
                                  Sort = i.Sort,
                                  UpdateTime = rtn.EndTime
                              }).ToList();
            is_del.AddRange(edit_rules);
            try
            {
                db.Ado.BeginTran();
                db.Updateable(hK).ExecuteCommand();
                db.Updateable(is_del).ExecuteCommand();
                db.Insertable(is_ins).ExecuteCommand();
                db.Ado.CommitTran();
                info.msg = "success";
            }
            catch (Exception ex)
            {

                db.Ado.RollbackTran();
                info.code = 400;
                info.msg = "SystemError";
            }
            return info;

        }
        /// <summary>
        /// 
        /// </summary>
        /// <param name="OpenId"></param>
        /// <param name="Id"></param>
        /// <param name="EventName">活动名称</param>
        /// <param name="PageIndex"></param>
        /// <param name="PageSize"></param>
        /// <returns></returns>
        [HttpGet]
        public MsgInfo<List<RtnGetEvent>> GetEventHK(string OpenId, int Id, string EventName, int PageIndex, int PageSize = 20)
        {
            MsgInfo<List<RtnGetEvent>> info = new();
            long _PassportId = IsLogin(OpenId);
            if (_PassportId <= 0)
            {
                info.code = 400;
                info.msg = "Please login";
                return info;
            }
            var par_user = db.Queryable<AdminUser>().Where(x => x.OpenId == OpenId).First();
            var my = db.Queryable<EventHK>()
                .WhereIF(Id != 0, x => x.Id == Id).WhereIF(!string.IsNullOrWhiteSpace(EventName), x => x.EventName.Contains(EventName))
                .Where(x => x.ManageUserPassportId == par_user.PassportId).ToList();
            var myrules = db.Queryable<EventRules>().Where(x =>x.IsValid==1&& my.Select(d => d.GUID).ToList().Contains(x.Guid)).ToList();
            var my_deta = db.Queryable<EventDetails>().Where(x => my.Select(f => f.GUID).ToList().Contains(x.Guid)).ToList();
            var game = db.Queryable<GameType>().ToList();
            List<RtnGetEvent> retuls = new List<RtnGetEvent>();

            my.ForEach(x =>
            {
                var gameName = game.Where(d => x.GameTypeId.Contains(d.Id.ToString())).Select(d=>d.GameName).ToList();
                var msg = string.Empty;
                foreach (var item in gameName)
                {
                    msg += item + ",";
                }
                if(!string.IsNullOrEmpty(msg))
                msg = msg.Substring(0, msg.Length - 1);
                RtnGetEvent rtn = new RtnGetEvent();

                rtn.IsAudit = x.IsAudit;
                var status = (x.IsValid == 1 ? "进行中" : "暂停中");
                status = (x.UpdateTime > DateTime.Now ? status : "已结束");
                rtn.IsValid = status;
                rtn.EndTime = x.UpdateTime;
                rtn.EventName = x.EventName;
                rtn.UserNum = x.UserNum;
                rtn.Id = x.Id;
                rtn.GameName = msg;
                rtn.GetRecord = my_deta.Where(d => d.Guid == x.GUID && d.IsType == 1).Count() + "/" + x.AllUserNum;
                rtn.SumPrice = my_deta.Where(d => d.Guid == x.GUID && d.IsType == 1).Sum(d => d.BetAmount);
                rtn.RulesType = x.RulesType;
                rtn.CoinType = x.CoinType;
                rtn.ToDayUserNum = x.ToDayUserNum;
                rtn.StaTime = x.AddTime;
                rtn.PassportId = x.PassportId;
                rtn.GameTypeId = x.GameTypeId;
                rtn.EventRules = myrules.Where(d => d.Guid == x.GUID).Select(d => new RtnEventRules()
                {
                    Id = d.Id,
                    BetAmount = d.BetAmount,
                    Reward = d.Reward,
                }).ToList();
                retuls.Add(rtn);
            });
            info.data = retuls.Skip((PageIndex - 1) * PageSize).Take(PageSize).ToList();
            info.data_count = retuls.Count;
            return info;
        }
        /// <summary>
        /// 获取完成活动的人员 需要审核
        /// </summary>
        /// <param name="OpenId"></param>
        /// <param name="PageIndex"></param>
        /// <param name="PageSize"></param>
        /// <returns></returns>
        [HttpGet]
        public MsgInfo<List<RtnEventDetails>> GetEventDetails(string OpenId, string EventName, int PageIndex, int PageSize = 20)
        {
            MsgInfo<List<RtnEventDetails>> info = new();
            long _PassportId = IsLogin(OpenId);
            if (_PassportId <= 0)
            {
                info.code = 400;
                info.msg = "Please login";
                return info;
            }

            var par_user = db.Queryable<AdminUser>().Where(x => x.OpenId == OpenId).First();
            var pas = db.Queryable<EventHK>().WhereIF(!string.IsNullOrEmpty(EventName), x => x.EventName.Contains(EventName))
                .Where(x =>  x.ManageUserPassportId == par_user.PassportId).ToList();
            var eve_dts = db.Queryable<EventDetails>().Where(d => pas.Select(x => x.GUID).Contains(d.Guid)).ToList();
            var u_info = db.Queryable<UserInfo>().Where(x => eve_dts.Select(d => d.PassportId).ToList().Contains(x.PassportId)).ToList();
            List<RtnEventDetails> list = new List<RtnEventDetails>();
            eve_dts.ForEach(x =>
            {
                string va = string.Empty;
                switch (x.IsType)
                {
                    case 0:
                        va = "未审核";
                        break;
                    case 1:
                        va = "已审核";
                        break;
                    case 2:
                        va = "已拒绝";
                        break;
                }
                RtnEventDetails rtn = new RtnEventDetails()
                {
                    Address = u_info.Where(d => d.PassportId == x.PassportId).FirstOrDefault()?.HashAddress,
                    Admin = par_user.UserName,
                    UserName = u_info.Where(d => d.PassportId == x.PassportId).FirstOrDefault()?.UserName,
                    Amount = x.BetAmount,
                    AuditType = x.IsValid == 0 ? "手动审核" : "自动审核",
                    CreateTime = x.AddTime,
                    UpdateTime = x.UpdateTime,
                    EventName = pas.Where(d => d.GUID == x.Guid).FirstOrDefault().EventName,
                    Id = x.Id,
                    Status = va
                };
                list.Add(rtn);
            });
            info.data = list.OrderByDescending(d => d.CreateTime).Skip((PageIndex - 1) * PageSize).Take(PageSize).ToList();
            info.data_count = list.Count;
            return info;
        }

        /// <summary>
        /// 审核人员 发送奖金
        /// </summary>
        /// <param name="OpenId"></param>
        /// <param name="id"></param>
        /// <param name="ShType">0审核：1：拒绝审核</param>
        /// <returns></returns>
        [HttpGet]
        public MsgInfo<string> EditEventde(string OpenId, int id, int ShType)
        {
            MsgInfo<string> info = new();
            long _PassportId = IsLogin(OpenId);
            if (_PassportId <= 0)
            {
                info.code = 400;
                info.msg = "Please login";
                return info;
            }
            var all_deta = db.Queryable<EventDetails>().Where(x => x.IsValid == 0).ToList();
            var eve_deta = all_deta.Where(x => x.Id == id).First();
            var wode = db.Queryable<EventHK>().Where(x => x.GUID == eve_deta.Guid).First();
            DateTime t = DateTime.Now;
            eve_deta.UpdateTime = t;

            if (ShType == 0)
            {
                var d = all_deta.Where(x => x.PassportId == eve_deta.PassportId &&
                x.IsType == 1 && x.Guid == wode.GUID).ToList();
                if (wode.ToDayUserNum <= all_deta.Where(x => x.PassportId == eve_deta.PassportId &&
                  x.IsType == 1 && x.Guid == wode.GUID).Count())
                {
                    info.msg = "Maximum number of collection today";
                    return info;
                }
                if (wode.UserNum <= all_deta.Where(x => x.PassportId == eve_deta.PassportId &&
                   x.IsType == 2 && x.Guid == wode.GUID).Count())
                {
                    info.msg = "The number of collection has been used up";
                    return info;
                }
                var order = t.Year.ToString() + t.Month.ToString() + t.Day.ToString() + t.Hour.ToString() + t.Minute.ToString() + t.Second.ToString() + t.Millisecond.ToString();
                eve_deta.OrderId = "Auto" + order;
                eve_deta.IsType = 1;
                var user = db.Queryable<UserInfo>().Where(x => x.PassportId == eve_deta.PassportId&&x.ManageUserPassportId==wode.ManageUserPassportId).First();
                var User_port = db.Queryable<EventHK>().Where(x => x.GUID == eve_deta.Guid).First();
         
                RechargeCashOrder od = new RechargeCashOrder()
                {
                    AddTime = DateTime.Now,
                    UpdateTime = DateTime.Now,
                    IsValid = 1,
                    Sort = 1,
                    Ip = YRHelper.GetClientIPAddress(HttpContext),

                    PassportId = user.PassportId,
                    OrderID = eve_deta.OrderId,
                    FromHashAddress = "",
                    ToHashAddress = user.HashAddress,
                    CoinNumber = eve_deta.BetAmount,
                    IsPayment = 0,//是否支付(0未支付，1已支付)
                    CoinType = User_port.CoinType,//币类别(0usdt 1其他)
                    RCType = 1,//充值提现(0充值 1提现)
                    ManageUserPassportId = (long)user.ManageUserPassportId,
                    block_hash = "",
                    block_number = ""//存储支付系统编号
                };
                try
                {
                    db.Ado.BeginTran();
                    db.Updateable(eve_deta).ExecuteCommand();

                    db.Insertable(od).ExecuteCommand();
                    db.Ado.CommitTran();
                }
                catch (Exception)
                {

                    db.Ado.RollbackTran();
                    info.code = 400;
                    info.msg = "SystemError";
                    return info;
                }

                PaySys paySys = db.Queryable<PaySys>().Where(d => d.PassportId == (user.ManageUserPassportId == 0 ? 502423741 : user.ManageUserPassportId) && d.product == User_port.CoinType.ToString()).First();

                try
                {
                    string result = PostWithdraws(eve_deta.OrderId, eve_deta.BetAmount, user.HashAddress, paySys.merchant_no, 1, paySys.apiKey); ;
                    JObject jo = JObject.Parse(result);
                    string _params = jo["params"].Value<string>();
                    int code = jo["code"].Value<int>();
                    string message = jo["message"].Value<string>();
                    int _timestamp = jo["timestamp"].Value<int>();
                    if (code == 200)
                        info.msg = "Success";
                    else
                    {
                        info.code = 400;
                        info.msg = message
;                    }

                }
                catch (Exception ex)
                {
                    LogHelper.Debug($"PostWithdraw:{ex.Message}   parmas|OrderID:{eve_deta.OrderId}|Amount:{eve_deta.BetAmount}|ToHashAddress:{user.HashAddress}|merchant_no:{paySys?.merchant_no}");

                    PostCashErrorOrder(eve_deta.OrderId, eve_deta.BetAmount, user.HashAddress, paySys?.merchant_no, User_port.CoinType.ToString(), "");

                }
            }
            else
            {
                eve_deta.IsType = 2;
                db.Updateable(eve_deta).ExecuteCommand();
                info.msg = "Success";
            }
          
            return info;

        }
        /// <summary>
        /// 打开tgbot程序
        /// </summary>
        /// <param name="OpenId"></param>
        /// <param name="Type">1开启0关闭</param>
        /// <returns></returns>
        [HttpGet]
        public MsgInfo<string> OpenBot(string OpenId,int Type=1)
        {
            MsgInfo<string> info = new();
            long _PassportId = IsLogin(OpenId);
            if (_PassportId <= 0)
            {
                info.code = 400;
                info.msg = "Please login";
                return info;
            }
            if(_PassportId== 999999999)
            {
               TGbotHelp.rabbitackconsu("");
                info.msg = "Success";
                return info;
            }
            var man = db.Queryable<AdminUser>().First(d => d.OpenId == OpenId);
            if (TGbotHelp.RestartStart(man.PassportId.ToString(), Type))
            {
                info.msg = "Success";
            }
            else
            {
                info.code = 400;
                info.msg = "Error";
            }
      
            return info;
        }
        /// <summary>
        /// 修改token
        /// </summary>
        /// <param name="OpenId"></param>
        /// <param name="tokens"></param>
        /// <returns></returns>
        [HttpGet]
        public MsgInfo<string> UpdateToken(string OpenId, string tokens)
        {
            MsgInfo<string> info = new();
            long _PassportId = IsLogin(OpenId);
            if (_PassportId <= 0)
            {
                info.code = 400;
                info.msg = "Please login";
                return info;
            }
            var role = db.Queryable<AdminRole>().Where(x => x.Id == 1 && x.ManageUserPassportId ==
            SqlFunc.Subqueryable<AdminUser>().Where(d => d.OpenId == OpenId).Select(d => d.PassportId)).ToList();
            if (role.Count > 0)
            {
                ExeConfigurationFileMap ecf = new ExeConfigurationFileMap();
                ecf.ExeConfigFilename = "";
                Configuration config = System.Configuration.ConfigurationManager.OpenMappedExeConfiguration(ecf, ConfigurationUserLevel.None);
                var keys = config.AppSettings.Settings.AllKeys.ToList();

                config.AppSettings.Settings["token"].Value = tokens;
                config.Save();
                info.msg = "Success";
            }
            else
            {
                info.code = 400;
                info.msg = "Not enough permissions";

            }
            return info;
        }

        /// <summary>
        /// 查询语种
        /// </summary>
        /// <param name="OpenId"></param>
        /// <param name="PageIndex"></param>
        /// <param name="PageSize"></param>
        /// <returns></returns>
        [HttpGet]
        public MsgInfo<List<ULang>> GetULang(string OpenId, int PageIndex, int PageSize = 20)
        {
            MsgInfo<List<ULang>> info = new();
            long _PassportId = IsLogin(OpenId);
            if (_PassportId <= 0)
            {
                info.code = 400;
                info.msg = "Please login";
                return info;
            }
            var lang = db.Queryable<ULang>().Where(x => x.IsValid == 1 && x.ManageUserPassportId == 502423741).ToList();
            info.data = lang.Skip(((PageIndex - 1) * PageSize)).Take(PageSize).ToList();
            info.data_count = lang.Count;
            return info;
        }
        /// <summary>
        /// 添加语种
        /// </summary>
        /// <param name="OpenId"></param>
        /// <param name="UserLangName">用户语言名称</param>
        /// <param name="UserLangDesc"></param>
        /// <param name="UserLangCallBack">用户语言回调</param>
        /// <returns></returns>
        [HttpGet]
        public MsgInfo<string> CreateULang(string OpenId, string UserLangName, string UserLangDesc,
            string UserLangCallBack)
        {
            MsgInfo<string> info = new();
            long _PassportId = IsLogin(OpenId);
            if (_PassportId <= 0)
            {
                info.code = 400;
                info.msg = "Please login";
                return info;
            }
            ULang lang = new ULang()
            {

                Ip = YRHelper.GetClientIPAddress(HttpContext),
                AddTime = DateTime.Now,
                IsValid = 1,
                UpdateTime = DateTime.Now,
                UserLangCallBack = UserLangCallBack,
                UserLangDesc = UserLangDesc,
                UserLangName = UserLangName
            };
            lang.ManageUserPassportId = db.Queryable<AdminUser>().Where(x => x.OpenId == OpenId).First().PassportId;

            if (db.Insertable<ULang>(lang).ExecuteCommand() > 0)
                info.msg = "Success";
            else
            {
                info.code = 400;
                info.msg = "Not enough permissions";
            }
            return info;

        }
        /// <summary>
        /// 修改语种
        /// </summary>
        /// <param name="OpenId"></param>
        /// <param name="Id"></param>
        /// <param name="UserLangName"></param>
        /// <param name="UserLangDesc"></param>
        /// <param name="UserLangCallBack"></param>
        /// <returns></returns>
        [HttpGet]
        public MsgInfo<string> UpdateULang(string OpenId, int Id, string UserLangName, string UserLangDesc,
            string UserLangCallBack)
        {
            MsgInfo<string> info = new();
            long _PassportId = IsLogin(OpenId);
            if (_PassportId <= 0)
            {
                info.code = 400;
                info.msg = "Please login";
                return info;
            }
            var dic = new Dictionary<string, object>();
            dic.Add("Id", Id);
            dic.Add("UpdateTime", DateTime.Now);
            dic.Add("Ip", YRHelper.GetClientIPAddress(HttpContext));
            if (!string.IsNullOrEmpty(UserLangName))
                dic.Add(nameof(UserLangName), UserLangName);
            if (!string.IsNullOrEmpty(UserLangCallBack))
                dic.Add(nameof(UserLangCallBack), UserLangCallBack);
            if (!string.IsNullOrEmpty(UserLangDesc))
                dic.Add(nameof(UserLangDesc), UserLangDesc);

            if (db.Updateable(dic).AS("ULang").WhereColumns("Id").ExecuteCommand() > 0)
                info.msg = "Success";
            else
            {
                info.code = 400;
                info.msg = "Not enough permissions";
            }
            return info;


        }
        /// <summary>
        /// 逻辑删除语种
        /// </summary>
        /// <param name="OpenId"></param>
        /// <param name="Id"></param>
        /// <returns></returns>
        [HttpGet]
        public MsgInfo<string> DeleteULang(string OpenId, int Id)
        {
            MsgInfo<string> info = new();
            long _PassportId = IsLogin(OpenId);
            if (_PassportId <= 0)
            {
                info.code = 400;
                info.msg = "Please login";
                return info;
            }
            var dic = new Dictionary<string, object>();
            dic.Add("Id", Id);
            dic.Add("UpdateTime", DateTime.Now);
            dic.Add("IsValid", 0);
            if (db.Updateable(dic).AS("ULang").WhereColumns("Id").ExecuteCommand() > 0)
                info.msg = "Success";
            else
            {
                info.code = 400;
                info.msg = "Not enough permissions";
            }
            return info;
        }
        /// <summary>
        /// 获取语言详情
        /// </summary>
        /// <param name="OpenId"></param>
        /// <param name="LangId">语言id</param>
        /// <param name="Ulangkey">可为空 根据key来搜索</param>
        /// <param name="PageIndex"></param>
        /// <param name="PageSize"></param>
        /// <returns></returns>
        [HttpGet]
        public MsgInfo<List<ULangDetails>> GetUlangDetails(string OpenId, int LangId,string Ulangkey, int IsKeyboard = 1, int PageIndex=1, int PageSize = 20)
        {
            MsgInfo<List<ULangDetails>> info = new();
            long _PassportId = IsLogin(OpenId);
            if (_PassportId <= 0)
            {
                info.code = 400;
                info.msg = "Please login";
                return info;
            }
            var array = new string[5] { "DSYX", "DXYS", "SWYX", "BJLYX", "LXKF" };
            var my = db.Queryable<ULangDetails>().WhereIF(!string.IsNullOrEmpty(Ulangkey),d=>d.ULangKey==Ulangkey).WhereIF(LangId != 0, x => x.ULangId == LangId).Where(x => x.IsValid == 1&&x.ParentId==0&&x.IsKeyboard== 1&&x.ManageUserPassportId==
            SqlFunc.Subqueryable<AdminUser>().Where(d=>d.OpenId==OpenId).Select(d=>d.PassportId)&&array.Contains(x.ULangKey)).ToList();

            info.data = my.Skip((PageIndex - 1) * PageSize).Take(PageSize).ToList();
            info.data_count = my.Count;
            return info;
        } 
        /// <summary>
        /// 获取语言二级菜单详情
        /// </summary>
        /// <param name="OpenId"></param>
        /// <param name="LangId">语言id</param>
        /// <param name="Ulangkey">可为空 根据key来搜索</param>
        /// <param name="IsKeyboard">是否为键盘 默认为1 1为键盘</param>
        /// <param name="PageIndex"></param>
        /// <param name="PageSize"></param>
        /// <returns></returns>
        [HttpGet]
        public MsgInfo<List<ULangDetails>> GetTwoUlangDetails(string OpenId,int LangId, string Ulangkey, int PageIndex = 1, int PageSize = 20)
        {
            MsgInfo<List<ULangDetails>> info = new();
            long _PassportId = IsLogin(OpenId);
            if (_PassportId <= 0)
            {
                info.code = 400;
                info.msg = "Please login";
                return info;
            }
            var my = db.Queryable<ULangDetails>().WhereIF(LangId != 0, x => x.ULangId == LangId).Where(x => x.IsValid == 1 && x.ParentId != 0 && x.IsKeyboard == 1).ToList();
            info.data = my.Skip((PageIndex - 1) * PageSize).Take(PageSize).ToList();
            info.data_count = my.Count;
            return info;

        }
        /// <summary>
        /// 查看语言详情 是键盘发送文字的详情
        /// </summary>
        /// <param name="OpenId"></param>
        /// <param name="LangId"></param>
        /// <param name="UlangKey"></param>
        /// <returns></returns>
        [HttpGet]
        public MsgInfo<ULangDetails> GetUlangDetailsChild(string OpenId,int LangId,string UlangKey)
        {
            
            MsgInfo<ULangDetails> info = new();
            long _PassportId = IsLogin(OpenId);
            if (_PassportId <= 0)
            {
                info.code = 400;
                info.msg = "Please login";
                return info;
            }
          
            var ulangdeial = db.Queryable<ULangDetails>().First(x => x.ULangKey == UlangKey && x.IsKeyboard == 0 && x.ULangId == LangId &&
           x.ManageUserPassportId == SqlFunc.Subqueryable<AdminUser>().Where(d => d.OpenId == OpenId).Select(d => d.PassportId));
            string msg = string.Empty;
            if (ulangdeial == null)
            {
                info.code = 400;
                return info;
            }
            switch (UlangKey)
            {
               
                case "LXKF":
                    var array1 = ulangdeial.ULangValue.Split(',');
                    foreach (var item in array1)
                    {
                        var arrat2 = item.Split('!');
                        for (int i = 0; i < arrat2.Length; i++)
                        {
                            if (i % 2 != 0)
                            {
                                msg += arrat2[i]+",";
                            }
                        }
                    }
                    ulangdeial.ULangValue = msg.Substring(0,msg.Length-1);
                    break;
            }
            info.data = ulangdeial;
            return info;
        }
        /// <summary>
        /// 返回图片
        /// </summary>
        /// <param name="OpenId"></param>
        /// <param name="UlangKey"></param>
        /// <returns></returns>
        [HttpGet]
        public object GetUlangDetailsChildIMG(string OpenId,  string UlangKey)
        {

           
            long _PassportId = IsLogin(OpenId);
            MsgInfo<object> info = new Model.MsgInfo<object>();

            if (_PassportId <= 0)
            {
                info.code = 400;
                info.msg = "Please login";
                return info;
            }
            var array = new string[4] { "DSYX", "DXYS", "SWYX", "BJLYX" };
            if (!array.Contains(UlangKey))
            {
                info.code = 400;
                info.msg = "Param Error";
                return info;
            }
            using (var sw = new FileStream(TGbotHelp.fileimg(UlangKey, _PassportId.ToString()), FileMode.Open))
            {
                var bytes = new byte[sw.Length];
                sw.Read(bytes, 0, bytes.Length);
                sw.Close();
                return File(bytes, @"image/jpg");
            }
        }
        /// <summary>
        /// 保存图片
        /// </summary>
        /// <param name="OpenId"></param>
        /// <param name="UlangKey"></param>
        /// <param name="formFile"></param>
        /// <returns></returns>
        [HttpPost]
        public MsgInfo<object> Imgupload()
        {
            var formFile = Request.Form.Files.FirstOrDefault();
            MsgInfo<object> info = new Model.MsgInfo<object>();
            var httpvalue=   Request.Form;
            if (httpvalue == null)
            {
                info.code = 400;
                info.msg = "The uploaded file does not have a suffix";
                return info;
            }
            string OpenId = string.Empty;
            string UlangKey = string.Empty;
            foreach (var item in httpvalue)
            {
                OpenId = (item.Key == nameof(OpenId)) ? item.Value : OpenId;
                UlangKey = (item.Key== nameof(UlangKey)) ? item.Value : UlangKey;
            }
            long _PassportId = IsLogin(OpenId);
            LogHelper.Debug($"Imgupload:{_PassportId}|{UlangKey}");

            if (_PassportId <= 0)
            {
                info.code = 400;
                info.msg = "Please login";
                return info;
            }

            if (formFile==null)
            {
                info.code = 400;
                info.msg = "File Is Null";
                return info;
            }
            var fileExtension = Path.GetExtension(formFile.FileName);
            const string fileFilt = ".jpg|.jpeg";
            if (fileExtension == null)
            {
                info.code = 400;
                info.msg = "The uploaded file does not have a suffix";
                return info;
            }
            if (fileFilt.IndexOf(fileExtension.ToLower(), StringComparison.Ordinal) <= -1)
            {
                info.code = 400;
                info.msg = "Please upload JPG image";
                return info;
            }
            
            //插入图片数据                 
            using (FileStream fs = System.IO.File.Open(TGbotHelp.fileimg(UlangKey, _PassportId.ToString()),FileMode.OpenOrCreate))
            {
                formFile.CopyTo(fs);
                
                fs.Flush();
            }
            info.data = "Success";
            return info;

        }

        /// <summary>
        /// 获取语言key
        /// </summary>
        /// <param name="OpenId"></param>
        /// <param name="ULangId">语种id</param>
        /// <returns></returns>
        [HttpGet]
        public async Task<MsgInfo<List<ULangDetails>>> GetUlangkeyvalue(string OpenId, int ULangId)
        {
            MsgInfo<List<ULangDetails>> info = new();
            long _PassportId = IsLogin(OpenId);
            if (_PassportId <= 0)
            {
                info.code = 400;
                info.msg = "Please login";
                return info;
            }
            info.data =await db.Queryable<ULangDetails>().Where(d => SqlFunc.Subqueryable<ULangKeyValue>().Select(d => d.ULangKey).Contains(d.ULangKey)&&d.ManageUserPassportId== 502423741&&d.IsKeyboard==1&&d.ULangId==ULangId)
                .Select(d=>new ULangDetails() { ULangKey=d.ULangKey, ULangValue=d.ULangValue}).ToListAsync();
            return info;
        }
        /// <summary>
        /// 添加语言详情
        /// </summary>
        /// <param name="OpenID"></param>
        /// <param name="ULangId">语言id</param>
        /// <param name="ULangKey">key</param>
        /// <param name="ULangValue">value</param>
        /// <param name="ULangMark">备注</param>
        /// <param name="IsKeyboard">是否为键盘（0不是，1是）</param>
        /// <returns></returns>
        [HttpGet]
        public MsgInfo<string> InsertUlangDetails(string OpenId, int ULangId, string ULangKey, string ULangValue
            , string ULangMark, int IsKeyboard, int IsTwo)
        {

            MsgInfo<string> info = new();
            long _PassportId = IsLogin(OpenId);
            if (_PassportId <= 0)
            {
                info.code = 400;
                info.msg = "Please login";
                return info;
            }

            ULangDetails uLang = new ULangDetails()
            {
                ULangId = ULangId,
                AddTime = DateTime.Now,
                IsKeyboard = IsKeyboard,
                IsValid = 1,
                ULangKey = ULangKey,
                ULangValue = ULangValue,
                ULangMark = ULangMark,
                UpdateTime = DateTime.Now,
                Ip = YRHelper.GetClientIPAddress(HttpContext),
                ParentId = IsTwo
            };
            var my = db.Queryable<AdminUser>().First(d => d.OpenId == OpenId).PassportId;
           
            if (db.Queryable<ULangDetails>().Any(d => d.ULangId == ULangId && d.ULangKey == ULangKey&&d.ParentId==IsTwo&&d.ManageUserPassportId==my))
            {
                info.msg = "existing";
            }
            uLang.ManageUserPassportId = db.Queryable<AdminUser>().Where(x => x.OpenId == OpenId).First().PassportId;
            if (db.Insertable(uLang).ExecuteCommand() > 0)
                info.msg = "Success";
            else
            {
                info.code = 400;
                info.msg = "Not enough permissions";
            }
            return info;
        }
        /// <summary>
        /// 一级按钮修改语言详情表
        /// </summary>
        /// <param name="OpenId"></param>
        /// <param name="Id"></param>
        /// <param name="ULangValue"></param>
        /// <param name="ULangMark">备注</param>
        /// <returns></returns>
        [HttpGet]
        public MsgInfo<string> UpdateULangDetails(string OpenId, int Id, string ULangValue,string ULangMark)
        {
            MsgInfo<string> info = new();
            long _PassportId = IsLogin(OpenId);
            if (_PassportId <= 0)
            {
                info.code = 400;
                info.msg = "Please login";
                return info;
            }
            var ips = YRHelper.GetClientIPAddress(HttpContext);
            var manan = db.Queryable<AdminUser>().First(x => x.OpenId == OpenId).PassportId;
            if (!string.IsNullOrEmpty(ULangValue))
            {
                Dictionary<string, object> dic = new Dictionary<string, object>();
                dic.Add("Id", Id);
                dic.Add("ULangValue", ULangValue);
                dic.Add("ULangMark", ULangMark??"");
                dic.Add("UpdateTime", DateTime.Now);
                redis.Del(manan + "Lang");

                db.Updateable(dic).AS("ULangDetails").WhereColumns("Id").ExecuteCommand();
                redis.Del(manan + "Lang");
            }
            else
            {
                info.msg = "Value not null";
            }


            return info;
        }

        /// <summary>
        /// 二级按钮修改语言详情表
        /// </summary>
        /// <param name="OpenId"></param>
        /// <param name="Id"></param>
        /// <param name="ULangValue"></param>
        /// <param name="ULangMark">备注</param>
        /// <returns></returns>
        [HttpPost]
        public MsgInfo<string> UpdateTwoULangDetails(RtnUpdateUlangDetails rtn)
        {
            MsgInfo<string> info = new();
            long _PassportId = IsLogin(rtn.OpenId);
            if (_PassportId <= 0)
            {
                info.code = 400;
                info.msg = "Please login";
                return info;
            }
            if (!string.IsNullOrEmpty(rtn.ULangValue))
            {
                string msg = string.Empty;
                var result= db.Queryable<ULangDetails>().First(d => d.Id == rtn.Id);
                switch (result.ULangKey)
                {
                    case "LXKF":
                        var array = rtn.ULangValue.Replace("，", ",").Split(',');
                        var array1 = result.ULangValue.Split(',');
                        foreach (var item in array1)
                        {
                            var arrat2 = item.Split('!');
                            for (int i = 0; i < arrat2.Length; i++)
                            {
                                msg = arrat2[i];
                                goto Sta;
                            }
                        }
                    Sta:
                        string value = string.Empty;
                        foreach (var item in array)
                        {
                            value += (msg+"!" + item + ",");
                        }
                        rtn.ULangValue = value.Substring(0, value.Length - 1);
                        break;
                }
                var my = db.Queryable<AdminUser>().First(x => x.OpenId == rtn.OpenId);
                redis.Del(my.PassportId + "Lang");
                Dictionary<string, object> dic = new Dictionary<string, object>();
                dic.Add("ULangValue", rtn.ULangValue);
                dic.Add("UpdateTime", DateTime.Now);
                dic.Add("Id", rtn.Id);
                db.Updateable<ULangDetails>(dic).WhereColumns("Id").ExecuteCommand();
                redis.Del(my.PassportId + "Lang");
            }
            else
            {
                info.msg = "Value not null";
            }
            return info;
        }

        /// <summary>
        /// 逻辑删除于言表
        /// </summary>
        /// <param name="OpenId"></param>
        /// <param name="Id"></param>
        /// <returns></returns>
        [HttpGet]
        public MsgInfo<string> DeleteULangDetails(string OpenId, int Id)
        {
            MsgInfo<string> info = new();
            long _PassportId = IsLogin(OpenId);
            if (_PassportId <= 0)
            {
                info.code = 400;
                info.msg = "Please login";
                return info;
            }
            var dic = new Dictionary<string, object>();
            dic.Add("Id", Id);
            dic.Add("UpdateTime", DateTime.Now);
            dic.Add("IsValid", 0);
            if (db.Updateable(dic).AS("ULangDetails").WhereColumns("Id").ExecuteCommand() > 0)
                info.msg = "Success";
            else
            {
                info.code = 400;
                info.msg = "Not enough permissions";
            }
            return info;
        }

        #region 任务调度
        private void IsPassport(List<UserInfo> list, UserInfo info, ref long passportId)
        {
            if (info == null) return;
            if (info.ParentId == 999999999)
            {
                passportId = info.PassportId;
            }
            else
            {
                var result = list.Where(d => d.PassportId == info.ParentId).FirstOrDefault();
                IsPassport(list, result, ref passportId);
            }
        }
        [HttpPost]
        public void Qubiz()
        {
            LogHelper.Warn("2");
            var eventhk = db.Queryable<EventHK>().Where(x => x.UpdateTime > DateTime.Now && x.IsValid == 1).ToList();
            var even = DateTime.Now;
            if (eventhk.Count > 0)
                even = eventhk.Min(d => d.AddTime);
            var bet = db.Queryable<Bet>().Where(d => d.AddTime > even).ToList().GroupBy(d => d.PassportId).ToList();
            var eventrule = db.Queryable<EventRules>().Where(d => eventhk.Select(a => a.GUID).Contains(d.Guid) && d.IsValid == 1).ToList();
            var game = db.Queryable<GameType>().ToList();
            var _info = db.Queryable<UserInfo>().ToList();
            AgentImp agen = new Eve0();
            foreach (var item in eventhk)
            {
                foreach (var sd in bet)
                {
                    if (item.ManageUserPassportId == (sd.ToList().FirstOrDefault().ManageUserPassportId == 0 ? 502423741 : sd.ToList().FirstOrDefault().ManageUserPassportId))
                    {
                        if (item.PassportId != "")
                        {
                            long passportId = 0;

                            var user = _info.First(x => x.PassportId == sd.Key);
                            IsPassport(_info, user, ref passportId);
                            if (!item.PassportId.Contains(passportId.ToString()) || passportId == 0) continue;
                        }
                        var gameid = game.Where(d => item.GameTypeId.Contains(d.Id.ToString())).ToList();
                        var list = sd.ToList().Where(d => gameid.Select(d => d.Id).Contains(d.GameDetailsId) && d.CoinType == item.CoinType).OrderByDescending(d => d.AddTime).ToList();

                        (EventBet, EventDetails) nb = new();
                        if (list.Where(d => d.AddTime >= item.AddTime).Count() == 0) continue;
                        agen.TZJL(list.Where(d => d.AddTime >= item.AddTime).ToList(), item, eventrule.Where(d => d.Guid == item.GUID).ToList(), db, ref nb);
                        if (nb.Item1 != null && nb.Item2 != null)
                        {

                            if (!string.IsNullOrEmpty(nb.Item2.OrderId))
                            {
                                LogHelper.Error("34");
                                var user = db.Queryable<UserInfo>().First(d => d.PassportId == nb.Item2.PassportId);
                                PaySys paySys = db.Queryable<PaySys>().Where(d => d.PassportId == (user.ManageUserPassportId == 0 ? 502423741 : user.ManageUserPassportId)).First();


                                try
                                {
                                    DateTime t = DateTime.Now;
                                    RechargeCashOrder od = new RechargeCashOrder()
                                    {
                                        AddTime = t,
                                        UpdateTime = t,
                                        IsValid = 1,
                                        Sort = 1,
                                        Ip = YRHelper.GetClientIPAddress(HttpContext),
                                        PassportId = user.PassportId,
                                        OrderID = nb.Item2.OrderId,
                                        FromHashAddress = "",
                                        ToHashAddress = user.HashAddress,
                                        CoinNumber = nb.Item2.BetAmount,
                                        IsPayment = 0,//是否支付(0未支付，1已支付)
                                        CoinType = item.CoinType,//币类别(0usdt 1其他)
                                        RCType = 1,//充值提现(0充值 1提现)
                                        ManageUserPassportId = (long)user.ManageUserPassportId,
                                        block_hash = "",
                                        block_number = ""//存储支付系统编号
                                    };

                                    db.Ado.BeginTran();
                                    db.Insertable(nb.Item1).ExecuteCommand();
                                    db.Insertable(nb.Item2).ExecuteCommand();
                                    db.Insertable(od).ExecuteCommand();
                                    db.Ado.CommitTran();
                                }
                                catch (Exception)
                                {
                                    db.Ado.RollbackTran();
                                    return;
                                }
                                try
                                {
                                    LogHelper.Warn("23131" + "这里" + paySys.apiKey + paySys.merchant_no);
                                    string result = PostWithdraws(nb.Item2.OrderId, nb.Item2.BetAmount, user.HashAddress, paySys.merchant_no, item.CoinType, paySys.apiKey); ;
                                    JObject jo = JObject.Parse(result);
                                    LogHelper.Warn(JsonConvert.SerializeObject(jo));
                                    int code = jo["code"].Value<int>();
                                    if (code != 200) return;

                                }
                                catch (Exception ex)
                                {
                                    LogHelper.Debug($"PostWithdraw:{ex.Message}   parmas|OrderID:{nb.Item2.OrderId}|Amount:{nb.Item2.BetAmount}|ToHashAddress:{user.HashAddress}|merchant_no:{paySys?.merchant_no}");

                                    PostCashErrorOrder(nb.Item2.OrderId, nb.Item2.BetAmount, user.HashAddress, paySys?.merchant_no, item.CoinType.ToString(), "");

                                }

                            }
                            else
                            {
                                try
                                {
                                    db.Ado.BeginTran();
                                    db.Insertable(nb.Item1).ExecuteCommand();
                                    db.Insertable(nb.Item2).ExecuteCommand();
                                    db.Ado.CommitTran();
                                }
                                catch (Exception)
                                {
                                    db.Ado.RollbackTran();
                                    return;
                                }
                            }


                        }
                    }
                }
            }

        }

        [HttpPost]
        private string PostWithdraws(string OrderID, decimal Amount, string ToHashAddress, string _merchant_no, int product, string apikey)
        {
            var timestamp = convert_time_int_to10(DateTime.Now);
            var pro_duct = (product == 1 ? "USDT-TRC20Payout" : "TRXPayout");
            paramsCash paramss = new paramsCash() { product = pro_duct, amount = Amount.ToString(), merchant_ref = OrderID, extra = new extras() { address = ToHashAddress } };
            var json = JsonConvert.SerializeObject(paramss);
            Dictionary<string, string> valuePairs = new Dictionary<string, string>();
            valuePairs.Add("merchant_no", _merchant_no);
            valuePairs.Add("timestamp", timestamp.ToString());
            valuePairs.Add("sign_type", "MD5");
            valuePairs.Add("params", json);
            var sg = _merchant_no + json + "MD5" + timestamp + apikey;
            valuePairs.Add("sign", YRHelper.get_md5_32(sg));


            string result = HttpHelper.Helper.PostMothss("https://api.paypptp.com/api/gateway/withdraw", valuePairs);
            return result;

        }
        [HttpGet]
        public string PostWithdrawss(string customerid, string _merchant_no, int product, string apikey)
        {
            var timestamp = convert_time_int_to10(DateTime.Now).ToString();
            var pro_duct = (product == 1 ? "USDT" : "TRX");
            paramsAddress paramss = new paramsAddress() { customerid = customerid, currency = pro_duct };
            var json = JsonConvert.SerializeObject(paramss);
            Dictionary<string, string> valuePairs = new Dictionary<string, string>();
            valuePairs.Add("merchant_no", _merchant_no);
            valuePairs.Add("timestamp", timestamp);
            valuePairs.Add("sign_type", "MD5");
            valuePairs.Add("params", json);
            var sg = _merchant_no + json + "MD5" + timestamp + apikey;
            valuePairs.Add("sign", YRHelper.get_md5_32(sg));


            string result = HttpHelper.Helper.PostMothss("https://api.paypptp.com/api/gateway/bind-customer", valuePairs);
            return result;

        }
        /// <summary>
        /// 启动任务调度
        /// </summary>
        [HttpGet]
        public void StartTake()
        {
            TGbotHelp.StartTaskscheduling();
        }
        #endregion
        #region 机器人方法
        /// <summary>
        /// 机器人代理详情方法
        /// </summary>
        /// <param name="chatid"></param>
        /// <param name="IsType"></param>
        /// <returns></returns>
        [HttpGet]
        public List<ProWatrDetails> GetProWatr(string chatid, long ManageUserPassportId)
        {
            var pro_pro = db.Queryable<UserInfo>().Where(x => x.ManageUserPassportId == (ManageUserPassportId == 0 ? 502423741 : ManageUserPassportId)).ToList();
            var my = pro_pro.Where(x => x.ParentId == pro_pro.Where(x => x.TgChatId == chatid).FirstOrDefault().PassportId).ToList();

            var game = db.Queryable<GameType>().ToList();

            List<ProWatrDetails> result = new List<ProWatrDetails>();
            my.ForEach(s =>
            {                    //查看自己有多少个代理
                List<UserInfo> _users = new();
                TreeInfo(pro_pro, s.PassportId, ref _users);
                //流水
                var rechag = db.Queryable<Bet>().Where(d => _users.Select(a => a.PassportId).Contains(d.PassportId) && d.BetResult != 2).ToList(); /*GetMyChildsAllT<Bet>("bet", str, x.PassportId, "1");*/

                //返回信息 外面的大层 
                ProWatrDetails water = new ProWatrDetails()
                {

                    UserName = s.UserName,
                    LV1 = _users.Where(x => x.RoleId != 100).Count(),
                    LV2 = _users.Where(x => x.RoleId != 100 && x.AddTime >= DateTime.Today).Count(),
                    LV3 = _users.Where(d => d.RoleId == 100).Count(),
                    LV4 = _users.Where(d => d.RoleId == 100 && d.AddTime >= DateTime.Today).Count(),
                    TGname = s.OtherMsg
                };

                ///盈利
                List<ReturnProPrices> returnPros = new List<ReturnProPrices>();
                /// 游戏的流水和盈利
                game.ForEach(x =>
                {
                    for (int i = 0; i < 2; i++)
                    {
                        ReturnProPrices returnPro = new ReturnProPrices();
                        returnPro.GameName = x.GameName;
                        if (i == 0)
                        {
                            returnPro.PriceUY = rechag.Where(d => d.GameDetailsId == x.Id && d.AddTime >= DateTime.Today.AddDays(-1) && d.AddTime < DateTime.Today && d.CoinType == 1).Sum(d => d.BetCoin);
                            returnPro.PriceUN = returnPro.PriceUY - rechag.Where(d => d.GameDetailsId == x.Id && d.BetResult == 0 && d.AddTime >= DateTime.Today.AddDays(-1) && d.AddTime < DateTime.Today && d.CoinType == 1).Sum(d => d.BetCoin);
                            returnPro.PriceTY = rechag.Where(d => d.GameDetailsId == x.Id && d.AddTime >= DateTime.Today.AddDays(-1) && d.AddTime < DateTime.Today && d.CoinType == 2).Sum(d => d.BetCoin);
                            returnPro.PriceTN = returnPro.PriceTY - rechag.Where(d => d.GameDetailsId == x.Id && d.BetResult == 0 && d.AddTime >= DateTime.Today.AddDays(-1) && d.AddTime < DateTime.Today && d.CoinType == 2).Sum(d => d.BetCoin);
                        }
                        else
                        {

                            returnPro.PriceUY = rechag.Where(d => d.GameDetailsId == x.Id && d.AddTime >= DateTime.Today && d.CoinType == 1).Sum(d => d.BetCoin);
                            returnPro.PriceUN = returnPro.PriceUY - rechag.Where(d => d.GameDetailsId == x.Id && d.BetResult == 0 && d.AddTime >= DateTime.Today && d.CoinType == 1).Sum(d => d.BetCoin);
                            returnPro.PriceTY = rechag.Where(d => d.GameDetailsId == x.Id && d.AddTime >= DateTime.Today && d.CoinType == 2).Sum(d => d.BetCoin);
                            returnPro.PriceTN = returnPro.PriceTY - rechag.Where(d => d.GameDetailsId == x.Id && d.BetResult == 0 && d.AddTime >= DateTime.Today && d.CoinType == 2).Sum(d => d.BetCoin);
                        }
                        returnPros.Add(returnPro);
                    }
                });
                water.GameType = returnPros;
                result.Add(water);
            });

            return result;
        }
        /// <summary>
        /// 获取代理流水
        /// </summary>
        /// <param name="chatid"></param>
        /// <returns></returns>
        [HttpGet]
        public ProWatrBot GetWatr(string chatid, long ManageUserPassportId)
        {


            var user = db.Queryable<UserInfo>().Where(x => x.ManageUserPassportId == (ManageUserPassportId == 0 ? 502423741 : ManageUserPassportId)).ToList();
            List<UserInfo> users = new List<UserInfo>();
            TreeInfo(user, user.FirstOrDefault(d => d.TgChatId == chatid).PassportId, ref users);


            var game = db.Queryable<GameType>().ToList();
            ProWatrBot result = new();
            List<Bet> list = db.Queryable<Bet>().Where(x => users.Select(d => d.PassportId).Contains(x.PassportId) && x.BetResult != 2).ToList();

            List<ReturnProPrices> returnPros = new List<ReturnProPrices>();
            game.ForEach(x =>
            {
                for (int i = 0; i < 2; i++)
                {
                    ReturnProPrices returnPro = new ReturnProPrices();
                    returnPro.GameName = x.GameName;
                    if (i == 0)
                    {
                        returnPro.PriceUY = list.Where(d => d.GameDetailsId == x.Id && d.AddTime >= DateTime.Today.AddDays(-1) && d.AddTime < DateTime.Today && d.CoinType == 1).Sum(d => d.BetCoin);
                        returnPro.PriceUN = returnPro.PriceUY - list.Where(d => d.GameDetailsId == x.Id && d.BetResult == 0 && d.AddTime >= DateTime.Today.AddDays(-1) && d.AddTime < DateTime.Today && d.CoinType == 1).Sum(d => d.BetCoin);
                        returnPro.PriceTY = list.Where(d => d.GameDetailsId == x.Id && d.AddTime >= DateTime.Today.AddDays(-1) && d.AddTime < DateTime.Today && d.CoinType == 2).Sum(d => d.BetCoin);
                        returnPro.PriceTN = returnPro.PriceTY - list.Where(d => d.GameDetailsId == x.Id && d.BetResult == 0 && d.AddTime >= DateTime.Today.AddDays(-1) && d.AddTime < DateTime.Today && d.CoinType == 2).Sum(d => d.BetCoin);
                    }
                    else
                    {

                        returnPro.PriceUY = list.Where(d => d.GameDetailsId == x.Id && d.AddTime >= DateTime.Today && d.CoinType == 1).Sum(d => d.BetCoin);
                        returnPro.PriceUN = returnPro.PriceUY - list.Where(d => d.GameDetailsId == x.Id && d.BetResult == 0 && d.AddTime >= DateTime.Today && d.CoinType == 1).Sum(d => d.BetCoin);
                        returnPro.PriceTY = list.Where(d => d.GameDetailsId == x.Id && d.AddTime >= DateTime.Today && d.CoinType == 2).Sum(d => d.BetCoin);
                        returnPro.PriceTN = returnPro.PriceTY - list.Where(d => d.GameDetailsId == x.Id && d.BetResult == 0 && d.AddTime >= DateTime.Today && d.CoinType == 2).Sum(d => d.BetCoin);
                    }
                    returnPros.Add(returnPro);
                }
            });
            result.GameType = returnPros;
            result.LV1 = users.Where(x => x.RoleId != 100).Count();
            result.LV2 = users.Where(x => x.AddTime >= DateTime.Today && x.RoleId != 100).Count();
            result.LV3 = users.Where(x => x.RoleId == 100).Count();
            result.LV4 = users.Where(x => x.AddTime >= DateTime.Today && x.RoleId == 100).Count();
            return result;
        }
        /// <summary>
        /// 返佣
        /// </summary>
        /// <param name="chatid"></param>
        /// <returns></returns>
        [HttpGet]
        public RtnReba GetRate(string chatid, long ManageUserPassportId)
        {
            var _user = db.Queryable<UserInfo>().Where(x => x.ManageUserPassportId == ManageUserPassportId).ToList();
            var my = _user.First(d => d.TgChatId == chatid);//本人
            var pro_pro = _user.Where(x => x.ParentId == my.PassportId).ToList();//一级
            var pro_pro2 = _user.Where(x => pro_pro.Select(d => d.PassportId).Contains(x.ParentId)).ToList();//二级
            var pro_pro3 = new List<UserInfo>();//其他及
            pro_pro2.ForEach(d =>
            {
                TreeInfo(_user, d.PassportId, ref pro_pro3);
            });
            var wt0 = db.Queryable<Bet>().Where(x => x.PassportId == my.PassportId && x.BetResult != 2).ToList();//我投
            var wt1 = db.Queryable<Bet>().Where(x => pro_pro.Select(d => d.PassportId).Contains(x.PassportId) && x.BetResult != 2).ToList();//一级头
            var wt2 = db.Queryable<Bet>().Where(x => pro_pro2.Select(d => d.PassportId).Contains(x.PassportId) && x.BetResult != 2).ToList();//二级投
            var wt3 = db.Queryable<Bet>().Where(x => pro_pro3.Select(d => d.PassportId).Contains(x.PassportId) && x.BetResult != 2).ToList();///其他投

            var fy = db.Queryable<RebateDetails>().Where(x => x.PassportId == my.PassportId).ToList();
            var fy0 = fy.Where(x => x.BetPassportId == my.PassportId).ToList();
            var fy1 = fy.Where(d => pro_pro.Select(x => x.PassportId).Contains(d.BetPassportId)).ToList();
            var fy2 = fy.Where(d => pro_pro2.Select(x => x.PassportId).Contains(d.BetPassportId)).ToList();
            var fy3 = fy.Where(d => pro_pro3.Select(x => x.PassportId).Contains(d.BetPassportId)).ToList();
            var sumprice = fy.Sum(d => d.RebateAmount);
            var txprice = fy.Where(d => d.CalculationState == 1).Sum(d => d.RebateAmount);
            var wprice = fy.Where(d => d.CalculationState == 0).Sum(d => d.RebateAmount);
            RtnReba rtn = new RtnReba()
            {
                LV1 = pro_pro.Count,
                LV2 = pro_pro2.Count,
                LV3 = pro_pro3.Count,
                LV0UBetPri = wt0.Where(x => x.CoinType == 1).Sum(d => d.BetCoin),
                LV0TBetPri = wt0.Where(x => x.CoinType == 2).Sum(d => d.BetCoin),
                LV1UBetPri = wt1.Where(x => x.CoinType == 1).Sum(d => d.BetCoin),
                LV1TBetPri = wt1.Where(x => x.CoinType == 2).Sum(d => d.BetCoin),
                LV2UBetPri = wt2.Where(x => x.CoinType == 1).Sum(d => d.BetCoin),
                LV2TBetPri = wt2.Where(x => x.CoinType == 2).Sum(d => d.BetCoin),
                LV3UBetPri = wt3.Where(x => x.CoinType == 1).Sum(d => d.BetCoin),
                LV3TBetPri = wt3.Where(x => x.CoinType == 2).Sum(d => d.BetCoin),

                LV0URate = fy0.Where(x => x.CoinType == 1).Sum(d => d.RebateAmount),
                LV1URate = fy1.Where(x => x.CoinType == 1).Sum(d => d.RebateAmount),
                LV2URate = fy2.Where(x => x.CoinType == 1).Sum(d => d.RebateAmount),
                LV3URate = fy3.Where(x => x.CoinType == 1).Sum(d => d.RebateAmount),
                LV0TRate = fy0.Where(x => x.CoinType == 2).Sum(d => d.RebateAmount),
                LV1TRate = fy1.Where(x => x.CoinType == 2).Sum(d => d.RebateAmount),
                LV2TRate = fy2.Where(x => x.CoinType == 2).Sum(d => d.RebateAmount),
                LV3TRate = fy3.Where(x => x.CoinType == 2).Sum(d => d.RebateAmount),
                SumURate = fy.Where(x => x.CoinType == 1).Sum(d => d.RebateAmount),
                TXURate = fy.Where(d => d.CalculationState == 1 && d.CoinType == 1).Sum(d => d.RebateAmount),
                WURate = fy.Where(d => d.CalculationState == 0 && d.CoinType == 1).Sum(d => d.RebateAmount),
                SumTRate = fy.Where(x => x.CoinType == 2).Sum(d => d.RebateAmount),
                TXTRate = fy.Where(d => d.CalculationState == 1 && d.CoinType == 2).Sum(d => d.RebateAmount),
                WTRate = fy.Where(d => d.CalculationState == 0 && d.CoinType == 2).Sum(d => d.RebateAmount)
            };
            return rtn;
        }
        /// <summary>
        /// 获取走势
        /// </summary>
        /// <param name="chatid"></param>
        /// <returns></returns>
        [HttpGet]
        public List<string> GetImg(string chatid, string GameName, long ManageUserPassportId)
        {
            var my = db.Queryable<UserInfo>().Where(d => d.TgChatId == chatid && d.ManageUserPassportId == ManageUserPassportId).First();
            var game = db.Queryable<GameType>().Where(d => d.GameName == GameName).First();
            var zs = db.Queryable<Bet>().Where(x => x.PassportId == my.PassportId && game.Id==(x.GameDetailsId)).OrderBy(x => x.AddTime, OrderByType.Desc).Take(80).ToList();
            Console.WriteLine(zs.Count());
            return zs.OrderBy(x => x.AddTime).Select(x => x.OpenResult).ToList();
        }
        /// <summary>
        /// tg获取基本信息
        /// </summary>
        /// <param name="chatid"></param>
        /// <returns></returns>
        [HttpGet]
        public RtnJBXXDTO GetJBXX(string chatid, long ManageUserPassportId)
        {
            var user = db.Queryable<UserInfo>().Where(x => x.ManageUserPassportId == ManageUserPassportId).ToList();
            var pro4 = new List<UserInfo>();
            var my = user.First(x => x.TgChatId == chatid);
            var vip = (user.Where(x => x.ParentId == my.PassportId && x.RoleId == 100).ToList());
            pro4.AddRange(vip);
            var pro1 = user.Where(x => x.ParentId == my.PassportId && x.RoleId != 100).ToList();


            var pro2 = user.Where(x => pro1.Select(d => d.PassportId).Contains(x.ParentId) && x.RoleId != 100).ToList();


            var pro3 = new List<UserInfo>();
            pro2.ForEach(x =>
            {
                TreeProInfo(user, x.PassportId, ref pro3);

            });
            foreach (var d in vip)
            {
                GetVip(user, d.PassportId, ref pro4);
            }



            RtnJBXXDTO rtn = new RtnJBXXDTO()
            {
                Address = my.HashAddress,
                TOP1 = pro1.Count,
                TOP2 = pro2.Count,
                TOP3 = pro3.Count,
                TOP4 = pro4.Count
            };
            rtn.TOPCOUNT = rtn.TOP1 + rtn.TOP2 + rtn.TOP3;
            return rtn;
        }

        private void GetVip(List<UserInfo> list, long passportid, ref List<UserInfo> reflist)
        {
            var result = list.Where(x => x.ParentId == passportid && x.RoleId == 100).ToList();
            foreach (var item in result)
            {
                reflist.Add(item);
                GetVip(list, item.PassportId, ref reflist);
            }
        }
        /// <summary>
        /// 我的盈利
        /// </summary>
        /// <param name="chatid"></param>
        /// <returns></returns>
        [HttpGet]
        public RtMyWatr GETMYProfit(string chatid, long ManageUserPassportId)
        {
            {
                var my = db.Queryable<UserInfo>().First(x => x.TgChatId == chatid && x.ManageUserPassportId == ManageUserPassportId);
                var bets = db.Queryable<Bet>().Where(x => x.PassportId == my.PassportId && x.BetResult != 2).ToList();
                var game = db.Queryable<GameType>().ToList();
                var top1 = bets.Where(x => x.CoinType == 1).Sum(x => x.BetCoin);
                var top2 = top1 - bets.Where(d => d.BetResult == 0 && d.CoinType == 1).Sum(x => x.BetCoin);
                var top3 = bets.Where(x => x.CoinType == 2).Sum(x => x.BetCoin);
                var top4 = top3 - bets.Where(d => d.BetResult == 0 && d.CoinType == 2).Sum(x => x.BetCoin);
                var ZT = bets.Where(x => x.AddTime >= DateTime.Today.AddDays(-1) && x.AddTime < DateTime.Today).ToList();
                var Today = bets.Where(x => x.AddTime >= DateTime.Today).ToList();
                RtMyWatr rt = new RtMyWatr() { SumUWatr = top1, SumUProfit = top2, SumTWatr = top3, SumTProfit = top4, Yesterday = new List<GameWatr>(), Today = new List<GameWatr>() };
                //rt.SumProfit = rt.SumProfit - bets.Where(d => d.BetResult == 0).Sum(d => d.BetCoin);
                game.ForEach(x =>
                {
                    var Y_terday = new GameWatr() { GameName = x.GameName, GameUWatrs = ZT.Where(d => d.GameDetailsId == x.Id && d.CoinType == 1).Sum(d => d.BetCoin), GameTWatrs = ZT.Where(d => d.GameDetailsId == x.Id && d.CoinType == 2).Sum(d => d.BetCoin) };
                    var To_day = new GameWatr() { GameName = x.GameName, GameUWatrs = Today.Where(d => d.GameDetailsId == x.Id && d.CoinType == 1).Sum(d => d.BetCoin), GameTWatrs = Today.Where(d => d.GameDetailsId == x.Id && d.CoinType == 2).Sum(d => d.BetCoin) };
                    Y_terday.GameUProfit = Y_terday.GameUWatrs - ZT.Where(d => d.GameDetailsId == x.Id && d.BetResult == 0 && d.CoinType == 1).Sum(d => d.BetCoin);
                    To_day.GameUProfit = To_day.GameUWatrs - Today.Where(d => d.GameDetailsId == x.Id && d.BetResult == 0 && d.CoinType == 1).Sum(d => d.BetCoin);
                    Y_terday.GameTProfit = Y_terday.GameTWatrs - ZT.Where(d => d.GameDetailsId == x.Id && d.BetResult == 0 && d.CoinType == 2).Sum(d => d.BetCoin);
                    To_day.GameTProfit = To_day.GameTWatrs - Today.Where(d => d.GameDetailsId == x.Id && d.BetResult == 0 && d.CoinType == 2).Sum(d => d.BetCoin);
                    rt.Today.Add(To_day);
                    rt.Yesterday.Add(Y_terday);
                });
                return rt;

            }
        }
        /// <summary>
        /// 代理
        /// </summary>
        /// <param name="list"></param>
        /// <param name="PassportId"></param>
        /// <param name="infos"></param>
        private void TreeProInfo(List<UserInfo> list, long PassportId, ref List<UserInfo> infos)
        {
            var result = list.Where(x => x.ParentId == PassportId && x.RoleId != 100).ToList();
            foreach (var item in result)
            {
                TreeProInfo(list, item.PassportId, ref infos);
                infos.Add(item);
            }
        }

        /// <summary>
        /// botsetting 设置页面的数据
        /// </summary>
        /// <param name="OpenId"></param>
        /// <param name="UlangId">展示的语言id</param>
        /// <returns></returns>
        [HttpGet]
        public MsgInfo<RtnBot> SetBotUlang(string OpenId, int UlangId = 1)
        {
            MsgInfo<RtnBot> info = new MsgInfo<RtnBot>();
            long _PassportId = IsLogin(OpenId);
            if (_PassportId <= 0)
            {
                info.code = 400;
                info.msg = "Please login";
                return info;
            }
            var my = db.Queryable<AdminUser>().First(d => d.OpenId == OpenId).PassportId;
            var ulang = db.Queryable<ULang>().Where(x => x.ManageUserPassportId == 502423741 && x.IsValid == 1).ToList();
        var myulang = ulang.Where(d => d.ManageUserPassportId == my).Select(d => new ULang() { Id = d.Id, IsValid = 0, UserLangName = d.UserLangName }).ToList();
        myulang.ForEach(d =>
        {
            ulang.Remove(ulang.First(s => s.Id == d.Id));
            ulang.Add(d);
        });
            var setulang = ulang.First(x => x.Id == UlangId);
            var uLangDetails = db.Queryable<ULangDetails>().Where(x => x.ManageUserPassportId == 502423741 && x.ULangId == setulang.Id && x.IsValid == 1 && x.IsKeyboard == 1).ToList();
            var myuLangDetails = db.Queryable<ULangDetails>().Where(x => x.ManageUserPassportId == my && x.ULangId == setulang.Id && x.IsValid == 1 && x.IsKeyboard == 1).ToList();
        myuLangDetails.ForEach(d =>
        {
            uLangDetails.Remove(uLangDetails.First(s => s.Id == d.Id));
            uLangDetails.Add(d);
        });
            RtnBot rtn = new RtnBot();
            rtn.Ulangs = ulang.Select(x => new RtnUlang() { UlangId = x.Id, UlangName = x.UserLangName, IsVaild=x.IsValid }).ToList();

            rtn.UlangPublic = uLangDetails.Where(x =>  x.ParentId == 0).Select(x => new RtnULangDetails() { ULangDetailsId = x.ULangKey, ULangDetailsName = x.ULangValue }).ToList();
            rtn.UlangGRZX = uLangDetails.Where(x => x.ParentId != 0).Select(x => new RtnULangDetails() { ULangDetailsId = x.ULangKey, ULangDetailsName = x.ULangValue }).ToList();
            info.data = rtn;
            return info;

        }
    /// <summary>
    /// 添加机器日语言1
    /// </summary>
    /// <param name="rtn"></param>
    /// <returns></returns>
        [HttpPost]
        public MsgInfo<string> CrateBot(RtnCreateBot rtn)
        {
            MsgInfo<string> info = new MsgInfo<string>();
            long _PassportId = IsLogin(rtn.OpenId);
            if (_PassportId <= 0)
            {
                info.code = 400;
                info.msg = "Please login";
                return info;
            }
            if (db.Queryable<BotSys>().Any(x => x.BotToken == rtn.BotToken&&x.IsValid==1))
            {
                info.code = 200;
                info.msg = "Robots already exist";
                return info;
            }
            var ManageUserPassportId = db.Queryable<AdminUser>().First(x => x.OpenId == rtn.OpenId).PassportId;
            var ips = YRHelper.GetClientIPAddress(HttpContext);
            var time = DateTime.Now;
            BotSys bot = new BotSys() { AddTime = time, BotToken = rtn.BotToken, Details = rtn.BotName ?? "", Ip = ips, IsValid = 0, ManageUserPassportId = ManageUserPassportId, UpdateTime = time, IsStart = 1 };

            var ulang = db.Queryable<ULang>().Where(x => rtn.Ulangs.Select(d => d.UlangId).Contains(x.Id)).Select(x => new ULang()
            {
                AddTime = DateTime.Now,
                Ip = ips,
                IsValid = 1,
                ManageUserPassportId = ManageUserPassportId,
                UpdateTime = time,
                UserLangCallBack = x.UserLangCallBack,
                UserLangDesc = x.UserLangDesc,
                UserLangName = x.UserLangName
            }).ToList();
            var ulanglist = db.Queryable<ULangDetails>().Where(x => x.ManageUserPassportId == 502423741 && ulang.Select(d => d.Id).Contains(x.ULangId)).ToList();
            var Exitsulang = ulanglist.Where(x => x.IsKeyboard == 1 && !rtn.UlangPublic.Select(d => d.ULangDetailsId).Contains(x.ULangKey)).ToList();
            ulanglist.RemoveAll(x => Exitsulang.Select(d => d.ULangKey).Contains(x.ULangKey));
            var ulangdetatils = ulanglist.Select(d => new ULangDetails() { ManageUserPassportId = ManageUserPassportId, ULangKey = d.ULangKey, AddTime = time, Ip = ips, IsKeyboard = d.IsKeyboard, IsValid = d.IsValid, ParentId = d.ParentId, ULangId = d.ULangId, ULangMark = d.ULangMark, ULangValue = d.ULangValue, UpdateTime = time }).ToList();
            try
            {
                db.Ado.BeginTran();
                db.Insertable(bot).ExecuteCommand();
                db.Insertable(ulangdetatils).ExecuteCommand();
                db.Ado.CommitTran();
                info.msg = "Success";
            }
            catch (Exception)
            {

                db.Ado.RollbackTran();

            }
            return info;
        }

        /// <summary>
        /// 添加机器人
        /// </summary>
        /// <param name="rtn"></param>
        /// <returns></returns>
        [HttpPost]
        public MsgInfo<string> CrateBots(RtnCreateBots rtn)
        {
            MsgInfo<string> info = new MsgInfo<string>();
            long _PassportId = IsLogin(rtn.OpenId);
            if (_PassportId <= 0)
            {
                info.code = 400;
                info.msg = "Please login";
                return info;
            }
            var ips = YRHelper.GetClientIPAddress(HttpContext);
            var time = DateTime.Now;
            var ManageUserPassportId = db.Queryable<AdminUser>().First(x => x.OpenId == rtn.OpenId).PassportId;
            if (rtn.Id == 0)
            {
                if (db.Queryable<BotSys>().Any(x => x.BotToken == rtn.BotToken && x.IsValid == 1))
                {
                    info.code = 400;
                    info.msg = "Robots already exist";
                    return info;
                }
                if (db.Queryable<BotSys>().Any(x => x.ManageUserPassportId == ManageUserPassportId))
                {
                    info.code = 400;
                    info.msg = "Only one bot can be created";
                    return info;
                }
               
                BotSys bot = new BotSys() { AddTime = time, BotToken = rtn.BotToken, ShopName = rtn.ShopName, BotName = rtn.BotName ?? "", Ip = ips, IsValid =1, ManageUserPassportId = ManageUserPassportId, UpdateTime = time, IsStart = 1, Details = "", UnBindHashAddress = rtn.UnBindHashAddress };
                var ulang = db.Queryable<ULang>().Where(x => x.ManageUserPassportId == 502423741).Select(x => new ULang()
                {
                    AddTime = DateTime.Now,
                    Ip = ips,
                    IsValid = 1,
                    ManageUserPassportId = ManageUserPassportId,
                    UpdateTime = time,
                    UserLangCallBack = x.UserLangCallBack,
                    UserLangDesc = x.UserLangDesc,
                    UserLangName = x.UserLangName
                }).ToList();
                var ulanglist = db.Queryable<ULangDetails>().Where(x => x.ManageUserPassportId == 502423741).Select(d => new ULangDetails()
                {
                    ManageUserPassportId = ManageUserPassportId,
                    ULangKey = d.ULangKey,
                    AddTime = time,
                    Ip = ips,
                    IsKeyboard = d.IsKeyboard,
                    IsValid = d.IsValid,
                    ParentId = d.ParentId,
                    ULangId = d.ULangId,
                    ULangMark = d.ULangMark,
                    ULangValue = d.ULangValue,
                    UpdateTime = time
                }).ToList();
                try
                {
                    db.Ado.BeginTran();
                    db.Insertable(bot).ExecuteCommand();
                    db.Insertable(ulang).ExecuteCommand();
                    db.Insertable(ulanglist).UseParameter().ExecuteCommand();
                    db.Ado.CommitTran();
                    TGbotHelp.directoryCopy(@"D:\502423741", @"D:\" + ManageUserPassportId);
                    TGbotHelp.StartBot(ManageUserPassportId.ToString(), rtn.BotToken);

                }
                catch (Exception)
                {
                    info.code = 400;
                    info.msg = "SystemError";
                    db.Ado.RollbackTran();
                    TGbotHelp.directoryCopy(@"D:\502423741", @"D:\" + ManageUserPassportId);
                    TGbotHelp.StartBot( ManageUserPassportId.ToString(), rtn.BotToken);
                }
            }
            else
            {
                info=UpdateBots(rtn);
            }
            return info;
        }

        /// <summary>
        /// 获取个人所有的机器人
        /// </summary>
        /// <param name="OpenId"></param>
        /// <returns></returns>
        [HttpGet]
        public MsgInfo<BotSys> GetBot(string OpenId)
        {
            MsgInfo<BotSys> info = new MsgInfo<BotSys>();
            long _PassportId = IsLogin(OpenId);
            if (_PassportId <= 0)
            {
                info.code = 400;
                info.msg = "Please login";
                return info;
            }
            var my = db.Queryable<AdminUser>().First(d => d.OpenId == OpenId).PassportId;
            var mybot=db.Queryable<BotSys>().First(x =>my == x.ManageUserPassportId && x.IsValid == 1);
            //if (mybot != null)
            //    mybot.Sort = TGbotHelp.ExisStart(my.ToString()) ? 1 : 0;
            info.data = mybot;
            return info;
        }
        /// <summary>
        /// 修改机器人资料
        /// </summary>
        /// <param name="OpenId"></param>
        /// <param name="id"></param>
        /// <param name="Token"></param>
        /// <param name="Details"></param>
        /// <returns></returns>
        [HttpPost]
        public MsgInfo<string> UpdateBots(RtnCreateBots rtn)
        {
            MsgInfo<string> info = new MsgInfo<string>();
           
            Dictionary<string, object> obj = new Dictionary<string, object>();
            if (!string.IsNullOrEmpty(rtn.BotToken))
            {
                if (db.Queryable<BotSys>().Any(x => x.BotToken == rtn.BotToken && x.IsValid == 1&&x.Id!=rtn.Id))
                {
                    info.code = 200;
                    info.msg = "Robots already exist";
                    return info;
                }
            }
                obj.Add("BotToken", rtn.BotToken);
            obj.Add("ShopName", rtn.ShopName);
            obj.Add("UnBindHashAddress", rtn.UnBindHashAddress);
             obj.Add("BotName", rtn.BotName);
            obj.Add("UpdateTime", DateTime.Now);
            obj.Add("Id", rtn.Id);
            if (db.Updateable(obj).AS("BotSys").WhereColumns("Id").ExecuteCommand() > 0)
            {
                info.msg = "Success";
                TGbotHelp.StartBot(db.Queryable<AdminUser>().First(d=>d.OpenId==rtn.OpenId).PassportId.ToString(), rtn.BotToken);
            }
            else
                info.msg = "SystemError";
            return info;
        }
        /// <summary>
        /// 授权机器日语言
        /// </summary>
        /// <param name="OpenId"></param>
        /// <param name="BotId">机器日的id</param>
        /// <param name="UlangArray">语种的id 1,2</param>
        /// <param name="UlangDetailsArray">语言详情的key "GRZX,WDTG"</param>
        /// <returns></returns>
        [HttpGet]
        public MsgInfo<string> Authorizationlanguage(string OpenId,string BotId, string UlangArray,string UlangDetailsArray)
        {
            MsgInfo<string> info = new MsgInfo<string>();
            long _PassportId = IsLogin(OpenId);
            if (_PassportId <= 0)
            {
                info.code = 400;
                info.msg = "Please login";
                return info;
            }
            var ulangarrays = UlangArray.Split(',').ToList();
            var ulangdetailsarrays = UlangDetailsArray.Split(',').ToList();
            var my = db.Queryable<AdminUser>().First(d => d.OpenId == OpenId);
            var ips = YRHelper.GetClientIPAddress(HttpContext);
            var ulang = db.Queryable<ULang>().Where(d => d.ManageUserPassportId == 502423741&&ulangarrays.Contains(d.Id.ToString())).Select(x=>new ULang() { Ip=ips, AddTime=DateTime.Now, IsValid=1, ManageUserPassportId=my.PassportId, UpdateTime=DateTime.Now, UserLangCallBack=x.UserLangCallBack
            , UserLangDesc=x.UserLangDesc, UserLangName=x.UserLangName}).ToList();
            var myulang = db.Queryable<ULang>().Where(d => d.ManageUserPassportId == my.PassportId).ToList();
            myulang.ForEach(d =>
            {
                if (ulang.Any(s => s.UserLangCallBack == d.UserLangCallBack))
                {
                    d.IsValid = 1;
                }
                else
                {
                    d.IsValid = 0;
                }
                d.UpdateTime = DateTime.Now;
            });
             ulang.RemoveAll(d => myulang.Select(s => s.UserLangCallBack).Contains(d.UserLangCallBack));
           


            var ulangdeials = db.Queryable<ULangDetails>().Where(d => ulangarrays.Contains(d.ULangId.ToString()) && d.ManageUserPassportId == 502423741).ToList();
            var myulangdeials = db.Queryable<ULangDetails>().Where(d => d.ManageUserPassportId == my.PassportId).ToList();
            myulangdeials.ToList().ForEach(d =>
            {
                d.UpdateTime = DateTime.Now;

                if (myulangdeials.Any(s => s.ULangKey == d.ULangKey))
                {
                    d.IsValid = 1;
                }
                else
                {
                    d.IsValid = 0;
                }
            });

            ulangdeials.RemoveAll(d => myulangdeials.Select(s => s.ULangKey).Contains(d.ULangKey));
            var insertulang = ulangdeials.Select(x => new ULangDetails()
            {
                 ManageUserPassportId=my.PassportId, ULangKey=x.ULangKey, AddTime=DateTime.Now, Ip=ips, IsKeyboard=x.IsKeyboard,ULangMark=x.ULangMark, IsValid=x.IsValid, ParentId=x.ParentId, ULangId=x.ULangId, ULangValue=x.ULangValue, UpdateTime=DateTime.Now
            }).ToList();
            try
            {
                db.Ado.BeginTran();
                db.Insertable(insertulang).ExecuteCommand();
                db.Updateable(myulangdeials).ExecuteCommand();

                db.Insertable(ulang).ExecuteCommand();
                db.Updateable(myulang).ExecuteCommand();

                db.Ado.CommitTran();
                info.msg = "Success";
            }
            catch (Exception)
            {
                db.Ado.RollbackTran();
                info.code = 400;
                info.msg = "SystemError";
            }
            return info;

        }
        /// <summary>
        /// 删除机器人
        /// </summary>
        /// <param name="OpenId"></param>
        /// <param name="id"></param>
        /// <returns></returns>
        [HttpGet]
        public MsgInfo<string> DeleteBot(string OpenId,int id)
        {

            MsgInfo<string> info = new MsgInfo<string>();
            long _PassportId = IsLogin(OpenId);
            if (_PassportId <= 0)
            {
                info.code = 400;
                info.msg = "Please login";
                return info;
            }
            Dictionary<string, object> obj = new Dictionary<string, object>();
            obj.Add("UpdateTime", DateTime.Now);
            obj.Add("Id", id);
            obj.Add("IsValid", 0);
            if (db.Updateable(obj).AS("BotSys").WhereColumns("Id").ExecuteCommand() > 0)
                info.msg = "Success";
            else
                info.msg = "SystemError";
            return info;
        }
        /// <summary>
        /// 获取一级代理接口
        /// </summary>
        /// <param name="OpenId"></param>
        /// <returns></returns>
        [HttpGet]
        public MsgInfo<List<UserInfo>> GetOneAgent(string OpenId)
        {
            MsgInfo<List<UserInfo>> info = new MsgInfo<List<UserInfo>>();
            long _PassportId = IsLogin(OpenId);
            if (_PassportId <= 0)
            {
                info.code = 400;
                info.msg = "Please login";
                return info;
            }
            info.data = db.Queryable<UserInfo>().Where(d =>d.ParentId==999999999&& d.ManageUserPassportId == SqlFunc.Subqueryable<AdminUser>().Where(s => s.OpenId == OpenId).Select(a => a.PassportId)).Select(d=>new UserInfo() { PassportId=d.PassportId, UserName=d.UserName}).ToList();
            return info;
        }

        /// <summary>
        /// 佣金提现
        /// </summary>
        /// <param name="ManageUserPassportId"></param>
        /// <param name="chatid"></param>
        /// <param name="cointype"></param>
        [HttpGet]
        public void TgBotWithdrawal(long ManageUserPassportId, string chatid,int cointype)
        {
            var my = db.Queryable<UserInfo>().First(x => x.TgChatId == chatid && x.ManageUserPassportId == ManageUserPassportId);
            var rebatlist=db.Queryable<RebateDetails>().Where(x => x.PassportId == my.PassportId && x.ManageUserPassportId == my.ManageUserPassportId&&x.CoinType==cointype&&x.SettlementState==0).ToList();
            var pay = db.Queryable<PaySys>().First(x => x.ManageUserPassportId == ManageUserPassportId);
            var array = rebatlist.Select(d => d.Id).ToList();
            var price = rebatlist.Sum(d => d.RebateAmount);
            DateTime t = DateTime.Now;
            var orderid = "Bate" + t.Year.ToString() + t.Month.ToString() + t.Day.ToString() + t.Hour.ToString() + t.Minute.ToString() + t.Second.ToString() + t.Millisecond.ToString(); ;
            RechargeCashOrder od = new RechargeCashOrder()
            {
                AddTime = t,
                UpdateTime = t,
                IsValid = 1,
                Sort = 1,
                Ip = YRHelper.GetClientIPAddress(HttpContext),
                PassportId = my.PassportId,
                OrderID = orderid,
                FromHashAddress = "",
                ToHashAddress = my.HashAddress,
                CoinNumber = price,
                IsPayment = 0,//是否支付(0未支付，1已支付)
                CoinType = cointype,//币类别(0usdt 1其他)
                RCType = 3,//充值提现(0充值 1提现)
                ManageUserPassportId = (long)my.ManageUserPassportId,
                block_hash = "",
                block_number = ""//存储支付系统编号
            };
            var rabt = new Rebate() { AddTime = t, UpdateTime = t, Ip = YRHelper.GetClientIPAddress(HttpContext), CoinType = cointype, OrderID = orderid, PassportId = my.PassportId, RebateAmount = price, IsValid = 1, SettlementState = 1 };


            //db.Insertable()
            string msg = string.Empty;

            try
            {
                var intlangs = redis.Get(chatid);
                var result = JsonConvert.DeserializeObject<PayTheGold>(PostWithdraws(orderid, price, my.HashAddress, pay.merchant_no, cointype, pay.apiKey));
                if (result.code == "200")
                {
                    od.product_ref = result.paramss.product_ref;
                    switch (intlangs)
                    {
                        case "1":
                            msg = "提现成功";
                            break;
                        case "2":
                            msg = "提現成功";
                            break;
                        case "3":
                            msg = "Withdrawal success";
                            break;
                        case "4":
                            msg = "ถอนสำเร็จ";
                            break;
                    }
                }
                else
                {
                    switch (intlangs)
                    {
                        case "1":
                            msg = "提现正在申请中";
                            break;
                        case "2":
                            msg = "提現正在申請中";
                            break;
                        case "3":
                            msg = "The withdrawal request is pending";
                            break;
                        case "4":
                            msg = "อยู่ระหว่างการขอถอนเงิน";
                            break;
                    }
                }
            }
            catch (Exception)
            {
                PostCashErrorOrder(orderid, price, my.HashAddress, pay.merchant_no, cointype.ToString(), pay.apiKey);

            }
            finally
            {
                db.Insertable(rabt).ExecuteCommand();
                db.Insertable<RechargeCashOrder>(od).ExecuteCommand();
                db.Updateable<RebateDetails>().SetColumns(d => d.SettlementState == 1 && d.UpdateTime == t).Where(d => array.Contains(d.Id)).ExecuteCommand();
                TelegramBotClient boot = new TelegramBotClient( db.Queryable<BotSys>().First(d => d.ManageUserPassportId == ManageUserPassportId).BotToken);
                boot.SendTextMessageAsync(chatid, msg).Wait() ;
            }


        }

        #endregion
        #region 佣金查看接口
        /// <summary>
        /// 佣金汇总接口
        /// </summary>
        /// <param name="OpenId"></param>
        /// <returns></returns>
        [HttpGet]
        public MsgInfo<RtnSumRabate> GetRateSum(string OpenId,int UserType)
        {
            MsgInfo<RtnSumRabate> info = new MsgInfo<RtnSumRabate>();
            long _PassportId = IsLogin(OpenId, UserType.ToString());
            if (_PassportId <= 0)
            {
                info.code = 400;
                info.msg = "Please login";
                return info;
            }
            var rabt = new List<RebateDetails>();
            if (UserType == 0)
            {
                rabt = db.Queryable<RebateDetails>().Where(x => x.ManageUserPassportId == _PassportId).ToList();
            }
            else
            {
                var my = db.Queryable<UserInfo>().First(x => x.OpenId == OpenId);
                var user = db.Queryable<UserInfo>().Where(x => x.ManageUserPassportId == my.ManageUserPassportId).ToList();
                List<UserInfo> list = new List<UserInfo>();
                TreeInfo(user, my.PassportId, ref list);
                rabt = db.Queryable<RebateDetails>().Where(x => list.Select(d => d.PassportId).Contains(x.PassportId) && x.ManageUserPassportId == my.ManageUserPassportId).ToList();
            }
            info.data = new RtnSumRabate();
            info.data.SumU = rabt.Where(x => x.CoinType == 1).Sum(x => x.RebateAmount);
            info.data.SumT = rabt.Where(x => x.CoinType == 2).Sum(x => x.RebateAmount);
            info.data.WithdrawalU = rabt.Where(x => x.SettlementState == 1 && x.CoinType == 1).Sum(x => x.RebateAmount);
            info.data.WithdrawalT = rabt.Where(x => x.SettlementState == 1 && x.CoinType == 2).Sum(x => x.RebateAmount);
            info.data.WaitwithdrawalU = rabt.Where(x => x.SettlementState == 0 && x.CoinType == 1).Sum(x => x.RebateAmount);
            info.data.WaitwithdrawalU = rabt.Where(x => x.SettlementState == 0 && x.CoinType == 2).Sum(x => x.RebateAmount);
            return info;

        }
        /// <summary>
        /// 佣金报表接口
        /// </summary>
        /// <param name="OpenId"></param>
        /// <param name="UserType">0商户，1代理</param>
        /// <param name="UserName">用户名</param>
        /// <param name="Address">钱包地址</param>
        /// <param name="StaTime">开始时间</param>
        /// <param name="EndTime">结束时间</param>
        [HttpGet]
        public MsgInfo<List<RtnRabateCount>> GetRateList(String OpenId, int UserType, string UserName, string Address, DateTime? StaTime, DateTime? EndTime, int index = 1, int size = 20)
        {
            MsgInfo<List<RtnRabateCount>> info = new();
            long _PassportId = IsLogin(OpenId, UserType.ToString());
            if (_PassportId <= 0)
            {
                info.code = 400;
                info.msg = "Please login";
                return info;
            }
            var r_char = new List<RechargeCashOrder>();
            var r_bate = new List<RebateDetails>();
            var user = new List<UserInfo>();
            if (UserType == 0)
            {
                user = new List<UserInfo>();
                if (!string.IsNullOrEmpty(UserName) || !string.IsNullOrEmpty(Address))
                {
                    user = db.Queryable<UserInfo>().WhereIF(!string.IsNullOrEmpty(UserName), x => x.UserName.Contains(UserName)).WhereIF(!string.IsNullOrEmpty(Address), x => x.HashAddress.Contains(Address)).ToList();
                }
                r_char = db.Queryable<RechargeCashOrder>().WhereIF(user.Count > 0, x => user.Select(d => d.PassportId).Contains(x.PassportId)).Where(x => x.ManageUserPassportId == _PassportId).WhereIF(StaTime != null && EndTime != null, x => StaTime <= x.AddTime && x.AddTime >= EndTime).ToList();
                r_bate = db.Queryable<RebateDetails>().Where(x => r_char.Select(d => d.AddTime).Contains(x.UpdateTime) && x.ManageUserPassportId == _PassportId).ToList();
                if (user.Count > 0)
                {
                    user = user.Where(x => r_char.Select(d => d.PassportId).Contains(x.PassportId)).ToList();
                }
                else
                {
                    user = db.Queryable<UserInfo>().Where(x => r_char.Select(d => d.PassportId).Contains(x.PassportId) && x.ManageUserPassportId == _PassportId).ToList();
                }
            }
            else
            {
                var my = db.Queryable<UserInfo>().First(x => x.OpenId == OpenId);
                user = db.Queryable<UserInfo>().Where(x => x.ManageUserPassportId == my.ManageUserPassportId).ToList();
                var whereuser = new List<UserInfo>();
                if (!string.IsNullOrEmpty(UserName) || !string.IsNullOrEmpty(Address))
                {
                    whereuser = user.WhereIF(!string.IsNullOrEmpty(UserName), x => x.UserName.Contains(UserName)).WhereIF(!string.IsNullOrEmpty(Address), x => x.HashAddress.Contains(Address)).ToList();
                }
                else
                {
                    whereuser = user;
                }
                List<UserInfo> list = new List<UserInfo>();
                whereuser.ForEach(x => TreeInfo(user, x.PassportId, ref list));
                r_char = db.Queryable<RechargeCashOrder>().Where(x => list.Select(d => d.PassportId).Contains(x.PassportId) && x.ManageUserPassportId == my.ManageUserPassportId).WhereIF(StaTime != null && EndTime != null, x => StaTime <= x.AddTime && x.AddTime >= EndTime).ToList();
                r_bate = db.Queryable<RebateDetails>().Where(x => r_char.Select(d => d.AddTime).Contains(x.UpdateTime) && x.ManageUserPassportId == my.ManageUserPassportId && x.SettlementState == 1).ToList();
                user = user.Where(x => r_char.Select(d => d.PassportId).Contains(x.PassportId)).ToList();
            }
            r_char = r_char.OrderByDescending(d => d.AddTime).ToList();
            List<RtnRabateCount> lists = new List<RtnRabateCount>();
            foreach (var item in user)
            {
                foreach (var ChildOreder in r_char.Where(d => d.PassportId == item.PassportId).ToList())
                {
                    for (int i = 0; i < 2; i++)
                    {
                        RtnRabateCount rtn = new RtnRabateCount() { Address = item.HashAddress, UserName = item.UserName };
                        var rabet = r_bate.Where(x => x.PassportId == ChildOreder.PassportId && x.UpdateTime == ChildOreder.AddTime).ToList();
                        if (!string.IsNullOrEmpty(ChildOreder.product_ref))
                        {
                            rtn.Status = true;
                            rtn.StatusTime = ChildOreder.UpdateTime;
                            rtn.ChekBate = "";
                        }
                        if (i == 1)
                        {
                            rtn.ChildBate = rabet.Where(x => x.BetPassportId != ChildOreder.PassportId && x.CoinType == 1).Sum(x => x.RebateAmount) + "USDT";
                            rtn.MyBate = rabet.Where(x => x.BetPassportId == ChildOreder.PassportId && x.CoinType == 1).Sum(x => x.RebateAmount) + "USDT"; 
                            rtn.SumPrice = r_bate.Where(x => x.CoinType == 1).Sum(d => d.RebateAmount) + "USDT";

                        }
                        else
                        {
                            rtn.ChildBate =  rabet.Where(x => x.BetPassportId != ChildOreder.PassportId && x.CoinType == 2).Sum(x => x.RebateAmount) + "TRX";
                            rtn.MyBate =rabet.Where(x => x.BetPassportId == ChildOreder.PassportId && x.CoinType == 2).Sum(x => x.RebateAmount) + "TRX";
                            rtn.SumPrice = r_bate.Where(x => x.CoinType == 2).Sum(d => d.RebateAmount) + "TRX";
                        }


                        lists.Add(rtn);

                    }
                   
                }
            }
            info.data = lists.Skip((index - 1) * size).Take(size).ToList();
            info.data_count = lists.Count;
            return info;
        }
        #endregion


        #region 站内信
        /// <summary>
        /// 发送或保存站内信
        /// </summary>
        /// <param name="OpenId"></param>
        /// <param name="id">如果是草稿箱这里赋值</param>
        /// <param name="ToolsId">站台id</param>
        /// <param name="Title">标题</param>
        /// <param name="PassportId">代理id 1,1格式</param>
        /// <param name="Body">内容</param>
        /// <param name="file">图片</param>
        /// <param name="status">状态1：是发送0 是草稿箱</param>
        /// <param name="filename">图片地址</param>
        /// <returns></returns>
        [HttpPost]
        public async Task<object> StandInsideLetter(string OpenId, int id, int ToolsId, string Title, string PassportId, string Body, IFormFile file, int status)
        {
            MsgInfo<object> info = new();
            long _PassportId = IsLogin(OpenId);
            if (_PassportId <= 0)
            {
                info.code = 400;
                info.msg = "Please login";
                return info;
            }

            TelegramBotClient boot = new TelegramBotClient((await db.Queryable<BotSys>().FirstAsync(d => d.ManageUserPassportId == _PassportId)).BotToken);
            string name = string.Empty;
            if (file != null)
            {
                name = AppDomain.CurrentDomain.BaseDirectory + "\\Img\\" + Guid.NewGuid().ToString() + file.FileName.Substring(file.FileName.Length - 4);
                if (!Directory.Exists(AppDomain.CurrentDomain.BaseDirectory + "\\Img"))
                    Directory.CreateDirectory(AppDomain.CurrentDomain.BaseDirectory + "\\Img");
                using (FileStream fs = System.IO.File.Open(name, FileMode.OpenOrCreate))
                {
                    file.CopyTo(fs);
                    fs.Flush();
                }
            }

            var task = new List<Task>();
            int tasknum = 0;
            List<string> array = new List<string>();
            if (!string.IsNullOrEmpty(PassportId))
                array = PassportId.Split(',').ToList();
            var list = db.Queryable<UserInfo>().Where(d => d.ManageUserPassportId == _PassportId).WhereIF(array.Count() > 0, x => array.Contains(x.PassportId.ToString())).Where(x => x.TgChatId != "").ToList();

            if (id == 0)
            {
                CommunicationRecords records = new CommunicationRecords()
                {
                    AddTime = DateTime.Now,
                    Ip = YRHelper.GetClientIPAddress(HttpContext),
                    IsValid = 1,
                    MsgBody = Body,
                    MsgImg = name,
                    MsgTitle = Title,
                    PassportId = PassportId,
                    Status = status,
                    ToolsId = ToolsId,
                    UpdateTime = DateTime.Now,
                    ManageUserPassportId=_PassportId
                };
                if (db.Insertable(records).ExecuteCommand() > 0 && status == 1)
                {

                    if (list.Count > 0 && list.Count <= 10) tasknum = 2;
                    else if (list.Count > 10 && list.Count <= 50) tasknum = 5;
                    else if (list.Count > 50 && list.Count <= 200) tasknum = 20;
                    else if (list.Count > 200) tasknum = 50;

                    int qrs = 0;
                    CunTask(tasknum, list.Count, ref qrs);
                    var passportId = new List<string>();

                    foreach (var item in list.Select(d => d.TgChatId).ToList())
                    {
                        passportId.Add(item);
                        if (passportId.Count == qrs)
                        {
                            switch (ToolsId)
                            {
                                case 1:
                                    task.Add(BotSend(name, Title, Body, passportId, boot));
                                    break;
                            }
                            passportId = new List<string>();
                        }
                    }
                    if (passportId.Count > 0)
                    {
                        switch (ToolsId)
                        {
                            case 1:
                                task.Add(BotSend(name, Title, Body, passportId, boot));
                                break;
                        }
                    }
                    await Task.WhenAll(task);
                }
            }
            else
            {
                var mycomm = await db.Queryable<CommunicationRecords>().FirstAsync(x => x.Id == id);
                mycomm.MsgImg = name;
                mycomm.UpdateTime = DateTime.Now;
                mycomm.ToolsId = ToolsId;
                mycomm.Status = status;
                mycomm.MsgBody = Body;
                mycomm.MsgTitle = Title;
                mycomm.PassportId = PassportId;
                if (db.Updateable(mycomm).ExecuteCommand() > 0 && status == 1)
                {
                    if (list.Count > 0 && list.Count <= 10) tasknum = 2;
                    else if (list.Count > 10 && list.Count <= 50) tasknum = 5;
                    else if (list.Count > 50 && list.Count <= 200) tasknum = 20;
                    else if (list.Count > 200) tasknum = 50;

                    int qrs = 0;
                    CunTask(tasknum, list.Count, ref qrs);
                    var passportId = new List<string>();

                    foreach (var item in list.Select(d => d.TgChatId).ToList())
                    {
                        passportId.Add(item);
                        if (passportId.Count == qrs)
                        {
                            switch (ToolsId)
                            {
                                case 1:
                                    task.Add(BotSend(name, Title, Body, passportId, boot));
                                    break;
                            }
                            passportId = new List<string>();
                        }
                    }
                    if (passportId.Count > 0)
                    {
                        switch (ToolsId)
                        {
                            case 1:
                                task.Add(BotSend(name, Title, Body, passportId, boot));
                                break;
                        }
                    }
                    await Task.WhenAll(task);
                }
            }
            return info;
        }
        private void CunTask(int num, int stanum, ref int countrs)
        {
            if (stanum % num == 1)
            {
                countrs = stanum / num;
            }
            else
            {
                stanum -= 1;
                CunTask(num, stanum, ref countrs);
            }
        }
        private async Task BotSend(string stream, string tilte, string body, List<string> passportid, TelegramBotClient boot)
        {
            string msg = (tilte + "\n\n\n" + body).Replace(".", "\\.").
                    Replace("-", "\\-").
                    Replace("+", "\\+").
                    Replace("(", "\\(").
                    Replace(")", "\\)").
                    Replace("*", "\\*")
                    .Replace("<", "\\<")
                    .Replace(">", "\\>");
            if (string.IsNullOrEmpty(stream))
                foreach (var item in passportid)
                    await boot.SendTextMessageAsync(chatId: item, text: msg, parseMode: ParseMode.MarkdownV2);
            else
                foreach (var item in passportid)
                    using (var stam = new FileStream(stream, FileMode.Open, FileAccess.ReadWrite, FileShare.ReadWrite))
                        await boot.SendPhotoAsync(chatId: item, photo: new Telegram.Bot.Types.InputFiles.InputOnlineFile(stam), caption: msg, parseMode: ParseMode.MarkdownV2);

        }
        /// <summary>
        /// 获取站台
        /// </summary>
        /// <param name="OpenId"></param>
        /// <returns></returns>
        [HttpGet]
        public async Task<MsgInfo<List<CommunicationTools>>> GetCoommunicationTools(string OpenId)
        {
            MsgInfo<List<CommunicationTools>> info = new MsgInfo<List<CommunicationTools>>();
            long _PassportId = IsLogin(OpenId);
            if (_PassportId <= 0)
            {
                info.code = 400;
                info.msg = "Please login";
                return info;
            }
            info.data = await db.Queryable<CommunicationTools>().Where(x => x.IsValid == 1).ToListAsync();
            info.data_count = info.data.Count;
           
            return info;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="OpenId"></param>
        /// <param name="Title"></param>
        /// <param name="StaTime"></param>
        /// <param name="EndTime"></param>
        /// <param name="index"></param>
        /// <param name="size"></param>
        /// <returns></returns>
        [HttpGet]
        public async Task<MsgInfo<List<RtnCommunRecords>>> GetCoommunicationRecords(string OpenId,string Title,DateTime? StaTime,DateTime? EndTime, int index=20,int size=1)
        {

            MsgInfo<List<RtnCommunRecords>> info = new MsgInfo<List<RtnCommunRecords>>();
            long _PassportId = IsLogin(OpenId);
            if (_PassportId <= 0)
            {
                info.code = 400;
                info.msg = "Please login";
                return info;
            }
            var mysend =await db.Queryable<CommunicationRecords>().WhereIF(!string.IsNullOrEmpty(Title), (x) => x.MsgTitle.Contains(Title))
            .WhereIF(StaTime != null && EndTime != null, (x) => x.UpdateTime >= StaTime && x.UpdateTime <= EndTime).Where((x) => x.ManageUserPassportId == _PassportId).ToListAsync();
            var mytool = await db.Queryable<CommunicationTools>().ToListAsync();
            var user =await db.Queryable<UserInfo>().Where(d => d.ManageUserPassportId == _PassportId).ToListAsync();
            List<RtnCommunRecords> list = new List<RtnCommunRecords>();
            mysend.ForEach(x =>
            {
                RtnCommunRecords rtn=new RtnCommunRecords() { Body=x.MsgBody, OperationName="Me", id=x.Id, Title=x.MsgTitle, UpdateTime=x.UpdateTime};
                switch (x.Status)
                {
                    case 1:
                        rtn.Ststus = "已发送";
                        break;
                    case 0:
                        rtn.Ststus = "草稿";
                        break;
                }
                rtn.ToolName = mytool.FirstOrDefault(d => d.Id == x.ToolsId).CommunicationName;
                if (string.IsNullOrEmpty(x.PassportId))
                {
                    rtn.UserName = "全部";
                }
                else
                {
                    rtn.UserName = JsonConvert.SerializeObject(user.Where(d=>x.PassportId.Contains(d.PassportId.ToString())).Select(d=>d.UserName).ToList());
                }
                list.Add(rtn);
            });
            info.data = list.Skip((index - 1) * size).Take(index).ToList();
            info.data_count = list.Count;
            return info;

        }

       
        #endregion


        #region 谷歌
        /// <summary>
        /// 发送谷歌验证码
        /// </summary>
        /// <param name="OpenId"></param>
        /// <returns></returns>
        [HttpGet]
        public async Task<MsgInfo<string>> SenEmail(string OpenId)
        {
            MsgInfo<string> info = new();
            long _PassportId = IsLogin(OpenId);
            if (_PassportId <= 0)
            {
                info.code = 400;
                info.msg = "Please login";
                return info;
            }
            var email = await db.Queryable<AdminUser>().FirstAsync(d => d.OpenId == OpenId);
            options.ToAddress = "lpw1109@icloud.com";
            var sendInfo = new SenderInfo
            {
                SenderEmail = options.FromAddress,
                SenderName = email.UserName,
            };
            string Randomemail = GoogleHelp.RandomEmail();
            var time = TimeSpan.FromMinutes(5);
            redis.Set(email.UserName, Randomemail, time);
            await _EmailService.SendAsync(options.ToAddress, "验证码", "尊敬的用户"+

"您的邮箱验证码为"+ Randomemail + "（有效期为5分钟）。" +

"为了您账户的安全，请勿将此验证码共享给他人。" +

"如果您需要进一步的协助，请随时与客服联系，或发送电邮至support@SGdover.com。" +

"祝好", false, sender: sendInfo); 
            
            return info;
        }
        /// <summary>
        /// 校验邮箱验证码
        /// </summary>
        /// <param name="OpenId"></param>
        /// <param name="EmailCode"></param>
        /// <returns>成功返回google二维码和密钥</returns>
        [HttpGet]
        public async Task<MsgInfo<object>> CheckEmail(string OpenId,string EmailCode)
        {
            MsgInfo<object> info = new();
            long _PassportId = IsLogin(OpenId);
            if (_PassportId <= 0)
            {
                info.code = 400;
                info.msg = "Please login";
                return info;
            }
            var email = await db.Queryable<AdminUser>().FirstAsync(d => d.OpenId == OpenId);
            if (!redis.Exists(email.UserName))
            {
                info.code = 400;
                info.msg = "The verification code does not exist";
                return info;

            }
            var _emailcode = redis.Get(email.UserName);
            if (_emailcode == EmailCode)
            {
                info.msg = "Success";
                    
                    info.data = GoogleHelp.SetCode(Request.Host.Host, email.UserName,out string key);
                 email.GoogleKey = key;
                email.IsGoogle=(email.IsGoogle==0?1:0);
                db.Updateable<AdminUser>(email).ExecuteCommand();
            }
            else
            {
                info.code = 400;
                info.msg = "Error";
            }
            return info;
        }
        /// <summary>
        /// 校验google验证码
        /// </summary>
        /// <param name="OpenId"></param>
        /// <param name="GoogleCode"></param>
        /// <returns></returns>
        [HttpGet]
        public MsgInfo<string> CheckGoogleCode(string OpenId,string GoogleCode)
        {
            MsgInfo<string> info = new();
            long _PassportId = IsLogin(OpenId);
            if (_PassportId <= 0)
            {
                info.code = 400;
                info.msg = "Please login";
                return info;
            }
       
            if (GoogleHelp.CheckCode(GoogleCode, db.Queryable<AdminUser>().First(x => x.OpenId == OpenId).GoogleKey))
            {
                info.msg = "Success";
            }
            else
            {
                info.code = 400;
                info.msg="Error";
            }
            return info;
        }
        /// <summary>
        /// 解除绑定
        /// </summary>
        /// <param name="OpenId"></param>
        /// <param name="password"></param>
        /// <param name="GoogleCode"></param>
        /// <returns></returns>
        [HttpGet]
        public async Task<MsgInfo<string>> Removebinding(string OpenId,string password,string GoogleCode)
        {
            MsgInfo<string> info = new();
            long _PassportId = IsLogin(OpenId);
            if (_PassportId <= 0)
            {
                info.code = 400;
                info.msg = "Please login";
                return info;
            }
            if (GoogleHelp.CheckCode(GoogleCode, db.Queryable<AdminUser>().First(x => x.OpenId == OpenId).GoogleKey))
            {
                var my =await db.Queryable<AdminUser>().FirstAsync(d => d.OpenId == OpenId&&MD5Help.EncryptString(password)==d.Pwd);
                if (my == null)
                    info.msg = "Failure";
                else
                {
                    info.msg = "Success";
                    my.UpdateTime = DateTime.Now;
                    my.IsGoogle = 0;
                    my.GoogleKey = "";
                    db.Updateable(my).ExecuteCommand();
                }
            }
            else
            {
                info.code = 400;
                info.msg = "Error";
            }
            return info;
        }
        /// <summary>
        /// 二级解绑
        /// </summary>
        /// <param name="OpenId"></param>
        /// <param name="EmailCode"></param>
        /// <returns></returns>
        [HttpGet]
        public async Task<MsgInfo<string>> RemoveTwobinding(string OpenId,  string EmailCode)
        {
            MsgInfo<string> info = new();
            long _PassportId = IsLogin(OpenId);
            if (_PassportId <= 0)
            {
                info.code = 400;
                info.msg = "Please login";
                return info;
            }
            var email = await db.Queryable<AdminUser>().FirstAsync(d => d.OpenId == OpenId);

            if (!redis.Exists(email.UserName))
            {
                info.code = 400;
                info.msg = "The verification code does not exist";
                return info;

            }
            var _emailcode = redis.Get(email.UserName);
            if (_emailcode == EmailCode)
            {
            
                    info.msg = "Success";
                    email.UpdateTime = DateTime.Now;
                    email.IsGoogle = 0;
                email.GoogleKey = "";
                    db.Updateable(email).ExecuteCommand();
                
            }
            else
            {
                info.code = 400;
                info.msg = "Error";
            }
            return info;
        }
        #endregion

    }

}
