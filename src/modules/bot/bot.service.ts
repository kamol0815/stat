import { Injectable, OnModuleDestroy, OnModuleInit } from '@nestjs/common';
import { Bot, Context, InlineKeyboard, InputFile, session, SessionFlavor, GrammyError } from 'grammy';
import { config, SubscriptionType } from '../../shared/config';
import { SubscriptionService } from './services/subscription.service';
import { SubscriptionMonitorService } from './services/subscription-monitor.service';
import { SubscriptionChecker } from './services/subscription-checker';
import logger from '../../shared/utils/logger';
import { IPlanDocument, Plan } from '../../shared/database/models/plans.model';
import { IUserDocument, UserModel } from '../../shared/database/models/user.model';
import { generatePaymeLink } from '../../shared/generators/payme-link.generator';
import {
  ClickRedirectParams,
  getClickRedirectLink,
} from '../../shared/generators/click-redirect-link.generator';
import { buildSubscriptionCancellationLink, buildSubscriptionManagementLink } from '../../shared/utils/payment-link.util';
import mongoose from "mongoose";
import { CardType, UserCardsModel } from "../../shared/database/models/user-cards.model";
import { FlowStepType, SubscriptionFlowTracker } from 'src/shared/database/models/subscription.follow.tracker';
import {
  Transaction,
  TransactionStatus,
} from '../../shared/database/models/transactions.model';
import { BotInteractionStatsModel } from '../../shared/database/models/bot-interaction-stats.model';
import { UZCARD_FREE_TRIAL_BASE_URL } from '../../shared/constants/bot-links.constant';
import { promises as fs } from 'fs';
import { join } from 'path';
import { createTrackingToken } from './utils/interaction-tracking.util';

interface SessionData {
  pendingSubscription?: {
    type: SubscriptionType;
  };
  hasAgreedToTerms?: boolean;
  selectedService: string;
  mainMenuMessageId?: number;
  pendingOnetimePlanId?: string;
}

type BotContext = Context & SessionFlavor<SessionData>;

type InteractionFlag =
  | 'started'
  | 'clickedFreeTrial'
  | 'openedTerms'
  | 'openedUzcard';

@Injectable()
export class BotService implements OnModuleInit, OnModuleDestroy {
  private bot: Bot<BotContext>;
  private subscriptionService: SubscriptionService;
  private subscriptionMonitorService: SubscriptionMonitorService;
  private subscriptionChecker: SubscriptionChecker;
  private readonly ADMIN_IDS = [1487957834, 7554617589, 85939027, 2022496528, 7789445876, 319324337, 1083408];
  private readonly subscriptionTermsLink: string;
  private introVideoBuffer?: Buffer;
  private introVideoFilename?: string;
  private readonly introVideoCandidates = ['qiz3.mp4', 'intro.mp4'];
  private introVideoFileId?: string;
  private cachedInviteLink?: string;


  constructor() {
    this.bot = new Bot<BotContext>(config.BOT_TOKEN);
    this.subscriptionService = new SubscriptionService(this.bot);
    this.subscriptionMonitorService = new SubscriptionMonitorService(this.bot);
    this.subscriptionChecker = new SubscriptionChecker(
      this.subscriptionMonitorService,
    );
    this.subscriptionTermsLink = this.resolveSubscriptionTermsLink();
    const staticLink = process.env.CHANNEL_STATIC_LINK?.trim();
    if (staticLink) {
      this.cachedInviteLink = staticLink;
      logger.info('Static channel invite link configured', { link: staticLink });
    }
    this.setupMiddleware();
    this.setupHandlers();
  }

  private buildCancellationNotice(telegramId?: number): string {
    const link = telegramId
      ? buildSubscriptionCancellationLink(telegramId)
      : undefined;

    if (link) {
      return `Obunani bekor qilish uchun <a href="${link}">bu havola</a> orqali ariza yuboring.`;
    }

    return 'Obunani bekor qilish uchun botdagi "Obuna holati" bo‘limi orqali qo‘llab-quvvatlashga murojaat qiling.';
  }

  private resolveSubscriptionTermsLink(): string {
    const configured = config.SUBSCRIPTION_TERMS_URL?.trim();
    if (configured) {
      return configured;
    }

    const derived = buildSubscriptionManagementLink('subscription/terms');
    if (derived) {
      return derived;
    }

    return 'https://telegra.ph/Yulduzlar-Bashorati--OMMAVIY-OFERTA-10-29';
  }

  async onModuleInit(): Promise<void> {
    // Start the bot asynchronously to avoid blocking application startup
    this.startAsync();
  }

  async onModuleDestroy(): Promise<void> {
    await this.stop();
  }

  public async start(): Promise<void> {
    this.subscriptionChecker.start();

    await this.bot.start({
      allowed_updates: [
        'message',
        'callback_query',
        'chat_join_request',
        'chat_member',
        'my_chat_member',
      ],
      onStart: () => {
        logger.info('Bot started');
      },
    });
  }

  public async stop(): Promise<void> {
    logger.info('Stopping bot...');
    await this.bot.stop();
  }

  async handleCardAddedWithoutBonus(userId: string, telegramId: number, cardType: CardType, plan: IPlanDocument, username?: string, selectedService?: string) {
    try {
      const user = await UserModel.findById(userId);
      if (!user) {
        return;
      }

      if (!plan) {
        return;
      }

      user.subscriptionType = 'subscription'
      user.save();

      // Create regular subscription without bonus
      const {
        user: subscription,
        wasKickedOut,
        success
      } = await this.subscriptionService.renewSubscriptionWithCard(
        userId,
        telegramId,
        cardType,
        plan,
        username,
        selectedService
      );

      if (success) {
        await this.revokeUserInviteLink(subscription, false);

        const inviteLink = await this.getPrivateLink();
        subscription.activeInviteLink = inviteLink;
        await subscription.save();

        const keyboard = new InlineKeyboard()
          .url("🔗 Kanalga kirish", inviteLink)
          .row()
          .text("📊 Obuna holati", "check_status");

        // Format the end date
        const endDate = new Date(subscription.subscriptionEnd);
        const endDateFormatted = `${endDate.getDate().toString().padStart(2, '0')}.${(endDate.getMonth() + 1).toString().padStart(2, '0')}.${endDate.getFullYear()}`;

        let messageText = `✅ To'lov muvaffaqiyatli amalga oshirildi va kartangiz saqlandi!\n\n` +
          `📆 Yangi obuna muddati: ${endDateFormatted} gacha\n\n` +
          `Quyidagi havola orqali kanalga kirishingiz mumkin:`;

        await this.bot.api.sendMessage(
          telegramId,
          messageText,
          {
            reply_markup: keyboard,
            parse_mode: "HTML"
          }
        );

      }

    } catch (error) {
      await this.bot.api.sendMessage(
        telegramId,
        "⚠️ Kartangiz qo'shildi, lekin obunani yangilashda xatolik yuz berdi. Iltimos, administrator bilan bog'laning. @sssupporttbot"
      );
    }


  }
  async handleAutoSubscriptionSuccess(userId: string, telegramId: number, planId: string, username?: string): Promise<void> {
    try {
      const plan = await Plan.findById(planId);

      if (!plan) {
        logger.error(`Plan with name 'Wrestling' not found in handleAutoSubscriptionSuccessForWrestling`);
        return;
      }

      await SubscriptionFlowTracker.create({
        telegramId,
        username,
        userId,
        step: FlowStepType.COMPLETED_SUBSCRIPTION,
      });

      const user = await UserModel.findById(userId);

      if (!user) {
        throw new Error('User not found');
      }


      const { user: subscription } = await this.subscriptionService.createSubscriptionWithCard(
        userId,
        plan,
        username,
        30
      );

      await this.revokeUserInviteLink(subscription, false);

      const inviteLink = await this.getPrivateLink();
      subscription.activeInviteLink = inviteLink;
      await subscription.save();

      const keyboard = new InlineKeyboard()
        .url("🔗 Kanalga kirish", inviteLink)
        .row()
        .text("🔙 Asosiy menyu", "main_menu");

      // Format end date in DD.MM.YYYY format
      const endDateFormatted = `${subscription.subscriptionEnd.getDate().toString().padStart(2, '0')}.${(subscription.subscriptionEnd.getMonth() + 1).toString().padStart(2, '0')}.${subscription.subscriptionEnd.getFullYear()}`;


      let messageText = `🎉 Tabriklaymiz! Yulduzlar bashorati obunasi muvaffaqiyatli faollashtirildi!\n\n`;

      messageText += `📆 Obuna muddati: ${endDateFormatted} gacha\n\n`;


      // if (wasKickedOut) {
      //     //TODO we aren't banning users so this is not necessary, but I am keeping them for now
      //     await this.bot.api.unbanChatMember(config.CHANNEL_ID, telegramId);
      //     messageText += `ℹ️ Sizning avvalgi bloklanishingiz bekor qilindi. ` +
      //         `Quyidagi havola orqali kanalga qayta kirishingiz mumkin:`;
      // } else {
      //     messageText += `Quyidagi havola orqali kanalga kirishingiz mumkin:`;
      // }

      messageText += `Quyidagi havola orqali kanalga kirishingiz mumkin:`;


      await this.bot.api.sendMessage(
        telegramId,
        messageText,
        {
          reply_markup: keyboard,
          parse_mode: "HTML"
        }
      );

    } catch (error) {

      // Send error message to user
      await this.bot.api.sendMessage(
        telegramId,
        "⚠️ Avtomatik to'lov faollashtirildi, lekin obunani faollashtirish bilan bog'liq muammo yuzaga keldi. Iltimos, administrator bilan bog'laning."
      );
    }

  }
  async handlePaymentSuccess(
    userId: string,
    telegramId: number,
    username?: string,
  ): Promise<void> {
    console.log('WATCH! @@@ handlePaymentSuccess is being called! ');

    try {
      const plan = await Plan.findOne({ name: 'Basic' });

      if (!plan) {
        logger.error('No plan found with name "Basic"');
        return;
      }

      const { user: subscription, wasKickedOut } =
        await this.subscriptionService.createSubscription(
          userId,
          plan,
          username,
        );

      await this.revokeUserInviteLink(subscription, false);

      subscription.subscriptionType = 'onetime';

      const inviteLink = await this.getPrivateLink();
      subscription.activeInviteLink = inviteLink;
      await subscription.save();

      const keyboard = new InlineKeyboard().url('🔗 Kanalga kirish', inviteLink);

      let messageText =
        `🎉 Tabriklaymiz! To'lov muvaffaqiyatli amalga oshirildi!\n\n` +
        `⏰ Obuna tugash muddati: ${subscription.subscriptionEnd.getDate().toString().padStart(2, '0')}.${(subscription.subscriptionEnd.getMonth() + 1).toString().padStart(2, '0')}.${subscription.subscriptionEnd.getFullYear()}\n\n`;

      if (wasKickedOut) {
        await this.bot.api.unbanChatMember(config.CHANNEL_ID, telegramId);
        messageText +=
          `ℹ️ Sizning avvalgi bloklanishingiz bekor qilindi. ` +
          `Quyidagi havola orqali kanalga qayta kirishingiz mumkin:`;
      } else {
        messageText += `Quyidagi havola orqali kanalga kirishingiz mumkin:`;
      }

      await this.bot.api.sendMessage(telegramId, messageText, {
        reply_markup: keyboard,
        parse_mode: 'HTML',
      });
      console.log('WATCH! @@@ handlePaymentSuccess sent the message');
    } catch (error) {
      logger.error('Payment success handling error:', error);
      await this.bot.api.sendMessage(
        telegramId,
        "⚠️ To'lov amalga oshirildi, lekin obunani faollashtirish bilan bog'liq muammo yuzaga keldi. Iltimos, administrator bilan bog'laning.",
      );
    }
  }

  async handleSubscriptionSuccess(
    userId: string,
    planId: string,
    bonusDays: number,
    selectedService: string,
  ): Promise<void> {
    let telegramId: number | undefined;

    logger.warn(
      `Selected service in handleSubscriptionSuccess ${selectedService}`,
    );
    try {
      const plan = await Plan.findById(planId);
      if (!plan) {
        logger.error('No plan found with name "Basic"');
        return;
      }

      const user = await UserModel.findById(userId);
      if (!user) {
        logger.error(`User not found with ID: ${userId}`);
        return;
      }

      telegramId = user.telegramId;
      if (!telegramId) {
        logger.error(`Telegram ID not found for user: ${userId}`);
        return;
      }

      const { user: subscription, wasKickedOut } =
        await this.subscriptionService.createBonusSubscription(
          userId,
          plan,
          bonusDays,
          user.username,
          'yulduz',
        );

      await this.revokeUserInviteLink(subscription, false);

      const inviteLink = await this.getPrivateLink();
      subscription.activeInviteLink = inviteLink;
      await subscription.save();

      const keyboard = new InlineKeyboard().url('🔗 Kanalga kirish', inviteLink);

      const bonusEndFormatted = `${subscription.subscriptionEnd.getDate().toString().padStart(2, '0')}.${(subscription.subscriptionEnd.getMonth() + 1).toString().padStart(2, '0')}.${subscription.subscriptionEnd.getFullYear()}`;

      let messageText =
        `🎉 Tabriklaymiz! UzCard orqali Yulduzlar bashorati uchun obuna muvaffaqiyatli faollashtirildi!\n\n` +
        `🎁 ${bonusDays} kunlik bonus: ${bonusEndFormatted} gacha\n\n`;

      if (wasKickedOut) {
        await this.bot.api.unbanChatMember(config.CHANNEL_ID, telegramId);
        messageText += `ℹ️ Sizning avvalgi bloklanishingiz bekor qilindi. `;
      }

      messageText += `Quyidagi havola orqali kanalga kirishingiz mumkin:`;

      await this.bot.api.sendMessage(telegramId, messageText, {
        reply_markup: keyboard,
        parse_mode: 'HTML',
      });

      logger.info(
        `UzCard subscription success handled for user ${userId} with ${bonusDays} bonus days`,
      );
    } catch (error) {
      logger.error(`Error in handleUzCardSubscriptionSuccess: ${error}`);
      if (telegramId) {
        await this.bot.api.sendMessage(
          telegramId,
          "⚠️ UzCard orqali obunani faollashtirishda xatolik. Iltimos, administrator bilan bog'laning.",
        );
      }
    }
  }

  async handlePaymentSuccessForUzcard(
    userId: string,
    telegramId: number,
    username?: string,
    // fiscalQr?: string | undefined,
    selectedService?: string,
  ): Promise<void> {
    logger.info(`Selected service on handlePaymentSuccess: ${selectedService}`);
    try {
      const serviceName = selectedService ?? 'yulduz';
      const plan = await this.resolvePlanByService(serviceName);

      const subscription = await this.subscriptionService.createSubscription(
        userId,
        plan,
        username,
      );

      let messageText: string = '';

      await this.revokeUserInviteLink(subscription.user, false);

      const inviteLink = await this.getPrivateLink();
      subscription.user.activeInviteLink = inviteLink;
      await subscription.user.save();
      const keyboard = new InlineKeyboard().url('🔗 Kanalga kirish', inviteLink);

      // if (fiscalQr) {
      //   keyboard.row().url("🧾 Chekni ko'rish", fiscalQr);
      // }

      const subscriptionEndDate = subscription.user.subscriptionEnd;

      messageText =
        `🎉 Tabriklaymiz! Yulduzlar bashorati uchun to'lov muvaffaqiyatli amalga oshirildi!\n\n` +
        `⏰ Obuna tugash muddati: ${subscriptionEndDate.getDate().toString().padStart(2, '0')}.${(subscriptionEndDate.getMonth() + 1).toString().padStart(2, '0')}.${subscriptionEndDate.getFullYear()}\n\n`;

      if (wasKickedOut) {
        messageText +=
          `ℹ️ Sizning avvalgi bloklanishingiz bekor qilindi. ` +
          `Quyidagi havola orqali kanalga qayta kirishingiz mumkin:`;
      } else {
        messageText += `Quyidagi havola orqali kanalga kirishingiz mumkin:`;
      }

      await this.bot.api.sendMessage(telegramId, messageText, {
        reply_markup: keyboard,
        parse_mode: 'HTML',
      });
    } catch (error) {
      logger.error(`Error in handlePaymentSuccessForUzcard: ${error}`);
      await this.bot.api.sendMessage(
        telegramId,
        "⚠️ To'lov amalga oshirildi, lekin obunani faollashtirish bilan bog'liq muammo yuzaga keldi. Iltimos, administrator bilan bog'laning. @sssupporttbot",
      );
    }
  }


  private async startAsync(): Promise<void> {
    try {
      await this.start();
    } catch (error) {
      logger.error('Failed to start bot:', error);
    }
  }

  // ... rest of your methods remain the same ...
  private setupMiddleware(): void {
    this.bot.use(
      session({
        initial(): SessionData {
          return {
            selectedService: 'yulduz',
            hasAgreedToTerms: false, // Initialize as false by default
          };
        },
      }),
    );
    this.bot.use((ctx, next) => {
      logger.info(`user chatId: ${ctx.from?.id}`);
      return next();
    });

    this.bot.catch((err) => {
      logger.error('Bot error:', err);
    });
  }

  private setupHandlers(): void {
    this.bot.command('start', this.handleStart.bind(this));
    this.bot.command('admin', this.handleAdminCommand.bind(this));
    this.bot.on('callback_query', this.handleCallbackQuery.bind(this));
    this.bot.on('chat_join_request', this.handleChatJoinRequest.bind(this));
  }

  private async handleCallbackQuery(ctx: BotContext): Promise<void> {
    if (!ctx.callbackQuery?.data) return;

    const data = ctx.callbackQuery.data;
    if (!data) return;

    if (data.startsWith('onetime|')) {
      const [, provider] = data.split('|');
      if (
        provider === 'uzcard' ||
        provider === 'payme' ||
        provider === 'click'
      ) {
        await this.handleOneTimePaymentProviderSelection(
          ctx,
          provider as 'uzcard' | 'payme' | 'click',
        );
      } else {
        await this.safeAnswerCallback(ctx, {
          text: "Noma'lum to'lov turi tanlandi.",
          show_alert: true,
        });
      }
      return;
    }

    if (data === 'main_menu') {
      ctx.session.hasAgreedToTerms = false;
    }

    const handlers: { [key: string]: (ctx: BotContext) => Promise<void> } = {
      payment_type_onetime: this.handleOneTimePayment.bind(this),
      payment_type_subscription: this.handleSubscriptionPayment.bind(this),
      back_to_payment_types: this.showPaymentTypeSelection.bind(this),
      subscribe: this.handleSubscribeCallback.bind(this),
      check_status: this.handleStatus.bind(this),
      cancel_subscription: this.handleSubscriptionCancellation.bind(this),
      payme_subscription_unavailable: this.handlePaymeSubscriptionUnavailable.bind(this),
      main_menu: this.showMainMenu.bind(this),
      confirm_subscribe_basic: this.confirmSubscription.bind(this),
      agree_terms: this.handleAgreement.bind(this),

      not_supported_international: async (ctx) => {
        await this.safeAnswerCallback(ctx, {
          text: "⚠️ Kechirasiz, hozircha bu to'lov turi mavjud emas.",
          show_alert: true,
        });
      },
    };

    const handler = handlers[data];
    if (handler) {
      await handler(ctx);
    }
  }

  private async showMainMenu(ctx: BotContext): Promise<void> {
    ctx.session.hasAgreedToTerms = false;

    const keyboard = new InlineKeyboard()
      .text("🎯 Obuna bo'lish", 'subscribe')
      .row()
      .text('📊 Obuna holati', 'check_status')
      .row()
      .text('🛑 Obunani bekor qilish', 'cancel_subscription');

    const message = `Assalomu alaykum, ${ctx.from?.first_name}! 👋\n\n Yulduzlar bashorati kontentiga xush kelibsiz 🏆\n\nQuyidagi tugmalardan birini tanlang:`;

    const chatId = ctx.chat?.id;

    if (!ctx.callbackQuery && chatId && ctx.session.mainMenuMessageId) {
      try {
        await ctx.api.deleteMessage(chatId, ctx.session.mainMenuMessageId);
      } catch (error) {
        logger.warn('Old menu message could not be deleted', {
          chatId,
          messageId: ctx.session.mainMenuMessageId,
          error,
        });
      }
    }

    if (ctx.callbackQuery) {
      try {
        await ctx.editMessageText(message, {
          reply_markup: keyboard,
          parse_mode: 'HTML',
        });
        const messageId = ctx.callbackQuery.message?.message_id;
        if (messageId) {
          ctx.session.mainMenuMessageId = messageId;
        }
      } catch (error) {
        logger.warn('Failed to edit main menu message, sending new message', {
          error,
        });
        if (chatId) {
          const sentMessage = await ctx.reply(message, {
            reply_markup: keyboard,
            parse_mode: 'HTML',
          });
          ctx.session.mainMenuMessageId = sentMessage.message_id;
        }
      }
      return;
    }

    const sentMessage = await ctx.reply(message, {
      reply_markup: keyboard,
      parse_mode: 'HTML',
    });
    ctx.session.mainMenuMessageId = sentMessage.message_id;
  }

  private async handleStart(ctx: BotContext): Promise<void> {
    ctx.session.hasAgreedToTerms = false;

    ctx.session.mainMenuMessageId = undefined;

    const videoPromise = this.sendIntroVideo(ctx).catch((error) => {
      logger.warn('Intro video send failed during /start', { error });
    });
    await this.createUserIfNotExist(ctx);
    await this.recordInteraction(ctx.from?.id, { started: true });
    await videoPromise;
  }

  private async preloadIntroVideo(force = false): Promise<void> {
    if (this.introVideoBuffer && this.introVideoFileId && !force) {
      return;
    }

    for (const candidate of this.introVideoCandidates) {
      const resolvedPath = join(process.cwd(), candidate);
      try {
        const fileBuffer = await fs.readFile(resolvedPath);
        this.introVideoBuffer = fileBuffer;
        this.introVideoFilename = candidate;
        this.introVideoFileId = undefined;
        logger.info('Preloaded intro video', { file: resolvedPath });
        return;
      } catch (error) {
        if (typeof (logger as any).debug === 'function') {
          (logger as any).debug('Intro video candidate not found', {
            candidate: resolvedPath,
            error,
          });
        }
      }
    }

    this.introVideoBuffer = undefined;
    this.introVideoFilename = undefined;
    logger.warn('Intro video file not found', {
      searched: this.introVideoCandidates.map((file) => join(process.cwd(), file)),
    });
  }

  private async sendIntroVideo(ctx: BotContext): Promise<void> {
    const chatId = ctx.chat?.id;
    if (!chatId) return;
    if (!this.introVideoBuffer || !this.introVideoFilename) {
      await this.preloadIntroVideo();
      if (!this.introVideoBuffer || !this.introVideoFilename) {
        logger.warn('Intro video still unavailable, skipping send');
        return;
      }
    }

    const keyboard = new InlineKeyboard().text(
      '🎁 30 kunlik ✅ bepul obunaga ega bo\'lish',
      'subscribe',
    );

    if (this.introVideoFileId) {
      try {
        await ctx.api.sendVideo(chatId, this.introVideoFileId, {
          reply_markup: keyboard,
        });
        return;
      } catch (error) {
        logger.warn('Failed to send intro video by file_id, falling back to upload', {
          chatId,
          error,
        });
        this.introVideoFileId = undefined;
      }
    }

    try {
      const message = await ctx.api.sendVideo(
        chatId,
        new InputFile(this.introVideoBuffer, this.introVideoFilename),
        {
          reply_markup: keyboard,
        },
      );

      const uploadedFileId = message.video?.file_id;
      if (uploadedFileId) {
        this.introVideoFileId = uploadedFileId;
        logger.info('Cached intro video file_id for faster reuse', {
          fileId: uploadedFileId,
        });
      } else {
        logger.warn('Intro video upload did not return file_id');
      }
    } catch (error) {
      logger.error('Failed to send intro video', { chatId, error });
    }
  }

  private async recordInteraction(
    telegramId: number | undefined,
    updates: Partial<Record<InteractionFlag, boolean>>,
  ): Promise<void> {
    if (!telegramId) {
      return;
    }

    const setUpdates: Record<string, boolean> = {};
    for (const [key, value] of Object.entries(updates)) {
      if (value) {
        setUpdates[key] = true;
      }
    }

    if (Object.keys(setUpdates).length === 0) {
      return;
    }

    try {
      await BotInteractionStatsModel.findOneAndUpdate(
        { telegramId },
        {
          $setOnInsert: { telegramId },
          $set: setUpdates,
        },
        { upsert: true, new: true },
      ).exec();
    } catch (error) {
      logger.warn('Failed to record interaction metric', {
        telegramId,
        updates: setUpdates,
        error,
      });
    }
  }

  private async handleStatus(ctx: BotContext): Promise<void> {
    try {
      const telegramId = ctx.from?.id;
      const user = await UserModel.findOne({ telegramId });

      if (!user) {
        await this.safeAnswerCallback(ctx, {
          text: "Foydalanuvchi ID'sini olishda xatolik yuz berdi.",
          show_alert: true,
        });
        return;
      }

      if (!user.subscriptionStart && !user.subscriptionEnd) {
        const keyboard = new InlineKeyboard()
          .text("🎯 Obuna bo'lish", 'subscribe')
          .row()
          .text('🔙 Asosiy menyu', 'main_menu');

        await ctx.editMessageText(
          "Siz hali obuna bo'lmagansiz 🤷‍♂️\nObuna bo'lish uchun quyidagi tugmani bosing:",
          { reply_markup: keyboard },
        );
        return;
      }

      const subscription = await this.subscriptionService.getSubscription(
        user._id as string,
      );

      if (!subscription) {
        const keyboard = new InlineKeyboard()
          .text("🎯 Obuna bo'lish", 'subscribe')
          .row()
          .text('🔙 Asosiy menyu', 'main_menu');

        await ctx.editMessageText(
          "Hech qanday obuna topilmadi 🤷‍♂️\nObuna bo'lish uchun quyidagi tugmani bosing:",
          { reply_markup: keyboard },
        );
        return;
      }

      const status = subscription.isActive ? '✅ Faol' : '❌ Muddati tugagan';
      const expirationLabel = subscription.isActive
        ? '⏰ Obuna tugash muddati:'
        : '⏰ Obuna tamomlangan sana:';

      let subscriptionStartDate = 'Mavjud emas';
      let subscriptionEndDate = 'Mavjud emas';

      if (subscription.subscriptionStart) {
        const d = subscription.subscriptionStart;
        subscriptionStartDate = `${d.getDate().toString().padStart(2, '0')}.${(d.getMonth() + 1).toString().padStart(2, '0')}.${d.getFullYear()}`;
      }
      if (subscription.subscriptionEnd) {
        const d = subscription.subscriptionEnd;
        subscriptionEndDate = `${d.getDate().toString().padStart(2, '0')}.${(d.getMonth() + 1).toString().padStart(2, '0')}.${d.getFullYear()}`;
      }

      const message = `🎫 <b>Obuna ma'lumotlari:</b>\n
📅 Holati: ${status}
📆 Obuna bo'lgan sana: ${subscriptionStartDate}
${expirationLabel} ${subscriptionEndDate}`;

      const keyboard = new InlineKeyboard();

      if (subscription.isActive) {
        await this.revokeUserInviteLink(subscription, false);

        const inviteLink = await this.getPrivateLink();
        subscription.activeInviteLink = inviteLink;
        await subscription.save();

        keyboard.row();
        keyboard.url('🔗 Kanalga kirish', inviteLink);
      } else {
        keyboard.text("🎯 Qayta obuna bo'lish", 'subscribe');
      }

      keyboard.row().text('🔙 Asosiy menyu', 'main_menu');

      await ctx.editMessageText(message, {
        reply_markup: keyboard,
        parse_mode: 'HTML',
      });
    } catch (error) {
      logger.error('Status check error:', error);
      await this.safeAnswerCallback(ctx, {
        text: 'Obuna holatini tekshirishda xatolik yuz berdi.',
        show_alert: true,
      });
    }
  }

  private async handleSubscribeCallback(ctx: BotContext): Promise<void> {
    try {
      const telegramId = ctx.from?.id;
      const user = await UserModel.findOne({ telegramId });
      if (!user) {
        await this.safeAnswerCallback(ctx, {
          text: "Foydalanuvchi ID'sini olishda xatolik yuz berdi.",
          show_alert: true,
        });
        return;
      }

      ctx.session.hasAgreedToTerms = false;

      const triggeredFromIntroVideo = Boolean(
        ctx.callbackQuery?.message?.video,
      );

      const subscription = await this.subscriptionService.getSubscription(
        user._id as string,
      );

      const hasActiveSubscriptionFromService = Boolean(subscription?.isActive);
      const hasActiveSubscriptionFromUser = this.userHasActiveSubscription(user);
      const subscriptionActive = hasActiveSubscriptionFromService || hasActiveSubscriptionFromUser;

      // Debug logging
      logger.info('Subscription check details', {
        telegramId,
        hasActiveSubscriptionFromService,
        hasActiveSubscriptionFromUser,
        subscriptionActive,
        userIsActive: user.isActive,
        userSubscriptionEnd: user.subscriptionEnd,
        subscriptionIsActive: subscription?.isActive,
        subscriptionEnd: subscription?.subscriptionEnd
      });

      if (subscriptionActive) {
        const endDateSource =
          subscription?.subscriptionEnd ?? user.subscriptionEnd ?? null;
        let formatted = 'aktiv';
        if (endDateSource) {
          const endDateObj =
            endDateSource instanceof Date
              ? endDateSource
              : new Date(endDateSource);
          formatted = `${endDateObj.getDate().toString().padStart(2, '0')}.${(endDateObj.getMonth() + 1)
            .toString()
            .padStart(2, '0')}.${endDateObj.getFullYear()}`;
        }

        // Create a keyboard to go back to main menu
        const keyboard = new InlineKeyboard()
          .text('📊 Obuna holati', 'check_status')
          .row()
          .text('🔙 Asosiy menyu', 'main_menu');

        const messageText = `✅ Kechirasiz, sizda allaqachon faol obuna mavjud!\n\n📅 Obuna muddati: ${formatted} gacha\n\n🔔 Qayta to'lov talab qilinmaydi. Obunangiz faol bo'lgan vaqt davomida barcha premium kontentlardan foydalanishingiz mumkin.`;

        // Try to edit the message if it's from a callback query, otherwise send a new message
        if (ctx.callbackQuery) {
          try {
            await ctx.editMessageText(messageText, {
              reply_markup: keyboard,
              parse_mode: 'HTML',
            });
          } catch (error) {
            logger.warn('Failed to edit message for active subscription, sending new message', {
              error,
            });
            await ctx.reply(messageText, {
              reply_markup: keyboard,
              parse_mode: 'HTML',
            });
          }
        } else {
          await ctx.reply(messageText, {
            reply_markup: keyboard,
            parse_mode: 'HTML',
          });
        }

        await this.safeAnswerCallback(ctx);
        return;
      }

      if (triggeredFromIntroVideo) {
        await this.recordInteraction(ctx.from?.id, {
          clickedFreeTrial: true,
        });
      }

      const selectedService = await this.selectedServiceChecker(ctx);
      const defaultPlan = await this.resolvePlanByService(selectedService);

      // Create trackable links for analytics
      const termsLink = this.createTrackableTermsLink(telegramId);
      const uzcardLink = this.createTrackableUzcardLink(telegramId, user._id.toString(), defaultPlan._id.toString(), selectedService);

      // Create dynamic cancellation link for the current user
      const cancellationLink = buildSubscriptionCancellationLink(telegramId);

      const keyboard = new InlineKeyboard()
        .url('📄 Foydalanish shartlari', termsLink)
        .row()
        .url('✅ Obuna bo\'lish💳 Uzcard/Humo ', uzcardLink);

      const termsMessage =
        "Suniy intelekt tarafidan yulduzlar bashoratiga obuna bo'ling va 30 kun davomida mutlaqo bepul foydalaning. Obuna bo'lishdan oldin to‘liq ofertani e'tibor bilan o‘qib chiqing. 30 kun bepul foydalanish muddati tugagach, oylik to‘lov 5 555 so‘mni tashkil etadi.\n\n" +
        `Obunani istalgan vaqtda bu <a href="${cancellationLink}">havola</a> orqali bekor qilishingiz mumkin.\n\n` +
        "⬇️ Tugmalar orqali shartlarni o'qing va Uzcard/Humo kartalarini kiritish orqali obunani faollashtiring.";

      const chatId = ctx.chat?.id;
      const messageCanBeEdited = Boolean(ctx.callbackQuery?.message?.text);

      if (messageCanBeEdited) {
        try {
          await ctx.editMessageText(termsMessage, {
            reply_markup: keyboard,
            parse_mode: 'HTML',
          });
        } catch (error) {
          logger.warn('Failed to edit message for terms, sending new message', {
            chatId,
            error,
          });
          await ctx.reply(termsMessage, {
            reply_markup: keyboard,
            parse_mode: 'HTML',
          });
        }
      } else {
        await ctx.reply(termsMessage, {
          reply_markup: keyboard,
          parse_mode: 'HTML',
        });
      }

      await this.safeAnswerCallback(ctx);
    } catch (error) {
      logger.error('Subscription plan display error:', error);
      await this.safeAnswerCallback(ctx, {
        text: "Obuna turlarini ko'rsatishda xatolik yuz berdi.",
        show_alert: true,
      });
    }
  }

  private async handleAgreement(ctx: BotContext): Promise<void> {
    try {
      const telegramId = ctx.from?.id;
      const user = await UserModel.findOne({ telegramId });
      if (!user) {
        await this.safeAnswerCallback(ctx, {
          text: "Foydalanuvchi ID'sini olishda xatolik yuz berdi.",
          show_alert: true,
        });
        return;
      }

      ctx.session.hasAgreedToTerms = true;

      await this.showPaymentTypeSelection(ctx);
    } catch (error) {
      await this.safeAnswerCallback(ctx, {
        text: "To'lov turlarini ko'rsatishda xatolik yuz berdi.",
        show_alert: true,
      });
    }
  }

  private async confirmSubscription(ctx: BotContext): Promise<void> {
    try {
      if (!ctx.session.hasAgreedToTerms) {
        await this.handleSubscribeCallback(ctx);
        return;
      }

      const telegramId = ctx.from?.id;
      const user = await UserModel.findOne({ telegramId: telegramId });
      if (!user) {
        await this.safeAnswerCallback(ctx, {
          text: "Foydalanuvchi ID'sini olishda xatolik yuz berdi.",
          show_alert: true,
        });
        return;
      }

      const plan = await Plan.findOne({
        name: 'Basic',
      });

      if (!plan) {
        logger.error('No plan found with name "Basic"');
        return;
      }

      try {
        const { user: subscription } =
          await this.subscriptionService.createSubscription(
            user._id as string,
            plan,
            ctx.from?.username,
          );

        await this.revokeUserInviteLink(subscription, false);

        const inviteLink = await this.getPrivateLink();
        subscription.activeInviteLink = inviteLink;
        await subscription.save();
        const keyboard = new InlineKeyboard().url('🔗 Kanalga kirish', inviteLink);

        const messageText =
          `🎉 Tabriklaymiz! Siz muvaffaqiyatli obuna bo'ldingiz!\n\n` +
          `⏰ Obuna tugash muddati: ${subscription.subscriptionEnd.getDate().toString().padStart(2, '0')}.${(subscription.subscriptionEnd.getMonth() + 1).toString().padStart(2, '0')}.${subscription.subscriptionEnd.getFullYear()}\n\n` +
          `Quyidagi havola orqali kanalga kirishingiz mumkin:\n\n`;

        await ctx.editMessageText(messageText, {
          reply_markup: keyboard,
          parse_mode: 'HTML',
        });
      } catch (error) {
        if (
          error instanceof Error &&
          error.message === 'User already has an active subscription'
        ) {
          const keyboard = new InlineKeyboard()
            .text('📊 Obuna holati', 'check_status')
            .row()
            .text('🔙 Asosiy menyu', 'main_menu');

          await ctx.editMessageText(
            '⚠️ Siz allaqon faol obunaga egasiz. Obuna holatini tekshirish uchun quyidagi tugmani bosing:',
            { reply_markup: keyboard },
          );
          return;
        }
        logger.error('Subscription confirmation error:', error);
        await this.safeAnswerCallback(ctx, {
          text: 'Obunani tasdiqlashda xatolik yuz berdi.',
          show_alert: true,
        });
      }
    } catch (error) {
      logger.error('Subscription confirmation error:', error);
      await this.safeAnswerCallback(ctx, {
        text: 'Obunani tasdiqlashda xatolik yuz berdi.',
        show_alert: true,
      });
    }
  }

  private async getPrivateLink(): Promise<string> {
    if (this.cachedInviteLink) {
      return this.cachedInviteLink;
    }

    try {
      const link = await this.bot.api.exportChatInviteLink(config.CHANNEL_ID);
      if (!link) {
        throw new Error('Exported invite link was empty');
      }
      this.cachedInviteLink = link;
      logger.info('Cached persistent channel invite link');
      return link;
    } catch (error) {
      logger.error('Error generating channel invite link', { error });
      throw error;
    }
  }

  private async resolvePlanByService(
    selectedService: string,
  ): Promise<IPlanDocument> {
    let plan = await Plan.findOne({ selectedName: selectedService }).exec();
    if (plan) {
      return plan;
    }

    const basePlan = await Plan.findOne({ name: 'Basic' }).exec();

    const planPayload: Record<string, unknown> = {
      name: basePlan?.name ?? 'Basic',
      selectedName: selectedService,
      duration: basePlan?.duration ?? 30,
      price: basePlan?.price ?? 5555,
    };

    if (basePlan?.user) {
      planPayload.user = basePlan.user;
    }

    plan = await Plan.create(planPayload);
    logger.warn('Created fallback plan for service', {
      selectedService,
      planId: plan._id,
    });
    return plan;
  }

  private async handlePaymeSubscriptionUnavailable(
    ctx: BotContext,
  ): Promise<void> {
    await this.safeAnswerCallback(ctx, {
      text: '⚠️ Tez orada bu Payme bilan obuna ishlaydi. Jarayonda.',
      show_alert: true,
    });
  }

  private async handleSubscriptionCancellation(ctx: BotContext): Promise<void> {
    try {
      const telegramId = ctx.from?.id;
      if (!telegramId) {
        await this.safeAnswerCallback(ctx, {
          text: "Foydalanuvchi ID'sini olishda xatolik yuz berdi.",
          show_alert: true,
        });
        return;
      }

      const user = await UserModel.findOne({ telegramId }).exec();
      if (!user) {
        await this.safeAnswerCallback(ctx, {
          text: 'Kechirasiz, siz tizimda ro‘yxatdan o‘tmagansiz.',
          show_alert: true,
        });
        return;
      }

      const activeSubscription = await this.subscriptionService.getSubscription(
        user._id as string,
      );

      if (!activeSubscription || !this.userHasActiveSubscription(activeSubscription)) {
        await this.safeAnswerCallback(ctx, {
          text: 'Kechirasiz, hozirda sizda faol obuna yo‘q.',
          show_alert: true,
        });
        return;
      }

      const link = buildSubscriptionCancellationLink(telegramId);
      if (!link) {
        await this.safeAnswerCallback(ctx, {
          text: 'Bekor qilish havolasini yaratib bo‘lmadi. Keyinroq urinib ko‘ring.',
          show_alert: true,
        });
        return;
      }

      const keyboard = new InlineKeyboard()
        .url('🛑 Obunani bekor qilish', link)
        .row()
        .text('🔙 Asosiy menyu', 'main_menu');

      await ctx.editMessageText(
        'Obunani bekor qilish uchun quyidagi havoladan foydalaning. Havola shaxsiy va faqat siz uchun amal qiladi.',
        {
          reply_markup: keyboard,
          parse_mode: 'HTML',
        },
      );
    } catch (error) {
      logger.error('Subscription cancellation link error:', error);
      await this.safeAnswerCallback(ctx, {
        text: 'Bekor qilish havolasini olishda xatolik yuz berdi.',
        show_alert: true,
      });
    }
  }

  private async handleSubscriptionPaymentWithSavedCard(
    ctx: BotContext,
    _provider: CardType,
  ): Promise<void> {
    await this.safeAnswerCallback(ctx, {
      text: 'Saqlangan karta orqali to\'lov hozircha qo\'llab-quvvatlanmaydi.',
      show_alert: true,
    });
  }

  private async createUserIfNotExist(ctx: BotContext): Promise<void> {
    const telegramId = ctx.from?.id;
    const username = ctx.from?.username;

    if (!telegramId) {
      return;
    }

    const user = await UserModel.findOne({ telegramId });
    if (!user) {
      const newUser = new UserModel({
        telegramId,
        username,
      });
      await newUser.save();
    } else if (username && user.username !== username) {
      user.username = username;
      await user.save();
    }
  }

  private async showPaymentTypeSelection(ctx: BotContext): Promise<void> {
    try {
      // Check if user has agreed to terms before proceeding
      if (!ctx.session.hasAgreedToTerms) {
        await this.handleSubscribeCallback(ctx);
        return;
      }

      const keyboard = new InlineKeyboard()
        .text('🔄 Obuna | 30 kun bepul', 'payment_type_subscription')
        .row()
        .text("💰 Bir martalik to'lov", 'payment_type_onetime')
        .row()
        .text("🌍 Xalqaro to'lov (Tez kunda)", 'not_supported_international')
        .row()
        .text('🔙 Asosiy menyu', 'main_menu');

      await ctx.editMessageText(
        "🎯 Iltimos, to'lov turini tanlang:\n\n" +
        "💰 <b>Bir martalik to'lov</b> - 30 kun uchun.\n\n" +
        "🔄 <b>30 kunlik (obuna)</b> - Avtomatik to'lovlarni yoqish.\n\n" +
        "🌍 <b>Xalqaro to'lov</b> - <i>Tez orada ishga tushuriladi!</i>\n\n" +
        "🎁 <b>Obuna to‘lov turini tanlang va 30 kunlik bonusni qo'lga kiriting!</b>",
        {
          reply_markup: keyboard,
          parse_mode: 'HTML',
        },
      );
    } catch (error) {
      await this.safeAnswerCallback(ctx, {
        text: "To'lov turlarini ko'rsatishda xatolik yuz berdi.",
        show_alert: true,
      });
    }
  }

  private async handleOneTimePayment(ctx: BotContext): Promise<void> {
    try {
      if (!ctx.session.hasAgreedToTerms) {
        await this.handleSubscribeCallback(ctx);
        return;
      }

      const telegramId = ctx.from?.id;
      const user = await UserModel.findOne({ telegramId: telegramId });
      if (!user) {
        await this.safeAnswerCallback(ctx, {
          text: "Foydalanuvchi ID'sini olishda xatolik yuz berdi.",
          show_alert: true,
        });
        return;
      }

      if (this.userHasActiveSubscription(user)) {
        const provider = await this.getLastSuccessfulProvider(
          user._id.toString(),
        );
        const message = this.getAlreadyPaidMessage(provider);

        await this.safeAnswerCallback(ctx, {
          text: message,
          show_alert: true,
        });
        await this.showMainMenu(ctx);
        return;
      }

      const selectedService = await this.selectedServiceChecker(ctx);

      const plan = await this.resolvePlanByService(selectedService);

      ctx.session.pendingOnetimePlanId = plan._id.toString();

      const keyboard = await this.getOneTimePaymentMethodKeyboard(
        ctx,
        plan,
        selectedService,
      );

      await ctx.editMessageText(
        "💰 <b>Bir martalik to'lov</b>\n\n" +
        "Iltimos, o'zingizga ma'qul to'lov turini tanlang:",
        {
          reply_markup: keyboard,
          parse_mode: 'HTML',
        },
      );
    } catch (error) {
      await this.safeAnswerCallback(ctx, {
        text: "To'lov turlarini ko'rsatishda xatolik yuz berdi.",
        show_alert: true,
      });
    }
  }

  private async handleSubscriptionPayment(ctx: BotContext): Promise<void> {
    try {
      if (!ctx.session.hasAgreedToTerms) {
        await this.handleSubscribeCallback(ctx);
        return;
      }

      const telegramId = ctx.from?.id;
      const user = await UserModel.findOne({ telegramId: telegramId });
      if (!user) {
        await this.safeAnswerCallback(ctx, {
          text: "Foydalanuvchi ID'sini olishda xatolik yuz berdi.",
          show_alert: true,
        });
        return;
      }

      // Check if user has active subscription
      const subscription = await this.subscriptionService.getSubscription(
        user._id as string,
      );

      const subscriptionActive =
        Boolean(subscription?.isActive) || this.userHasActiveSubscription(user);

      if (subscriptionActive) {
        const endDateSource =
          subscription?.subscriptionEnd ?? user.subscriptionEnd ?? null;
        let formatted = 'aktiv';
        if (endDateSource) {
          const endDateObj =
            endDateSource instanceof Date
              ? endDateSource
              : new Date(endDateSource);
          formatted = `${endDateObj.getDate().toString().padStart(2, '0')}.${(endDateObj.getMonth() + 1)
            .toString()
            .padStart(2, '0')}.${endDateObj.getFullYear()}`;
        }

        // Create a keyboard to go back to main menu
        const keyboard = new InlineKeyboard()
          .text('📊 Obuna holati', 'check_status')
          .row()
          .text('🔙 Asosiy menyu', 'main_menu');

        const messageText = `✅ Kechirasiz, sizda allaqachon faol obuna mavjud!\n\n📅 Obuna muddati: ${formatted} gacha\n\n🔔 Qayta to'lov talab qilinmaydi. Obunangiz faol bo'lgan vaqt davomida barcha premium kontentlardan foydalanishingiz mumkin.`;

        await ctx.editMessageText(messageText, {
          reply_markup: keyboard,
          parse_mode: 'HTML',
        });

        await this.safeAnswerCallback(ctx);
        return;
      }

      const userId = user._id as string;

      await this.selectedServiceChecker(ctx);

      const keyboard = await this.getSubscriptionPaymentMethodKeyboard(
        userId,
        ctx,
      );

      await ctx.editMessageText(
        "🔄 <b>Avtomatik to'lov (obuna)</b>\n\n" +
        "Iltimos, to'lov tizimini tanlang. Har 30 kunda to'lov avtomatik ravishda amalga oshiriladi:",
        {
          reply_markup: keyboard,
          parse_mode: 'HTML',
        },
      );
    } catch (error) {
      await this.safeAnswerCallback(ctx, {
        text: "To'lov turlarini ko'rsatishda xatolik yuz berdi.",
        show_alert: true,
      });
    }
  }

  private async getOneTimePaymentMethodKeyboard(
    ctx: BotContext,
    _plan: IPlanDocument,
    selectedService: string,
  ) {
    if (!selectedService) {
      await this.safeAnswerCallback(ctx, { text: 'Iltimos, avval xizmat turini tanlang.', show_alert: true });
      await this.showMainMenu(ctx);
      return;
    }

    return new InlineKeyboard()
      .text("📲 Uzcard orqali to'lash", this.buildOneTimePaymentCallback('uzcard'))
      .row()
      .text("📲 Payme orqali to'lash", this.buildOneTimePaymentCallback('payme'))
      .row()
      .text("💳 Click orqali to'lash", this.buildOneTimePaymentCallback('click'))
      .row()
      .text('🔙 Asosiy menyu', 'main_menu');
  }

  private async getSubscriptionPaymentMethodKeyboard(
    userId: string,
    ctx: BotContext,
  ) {
    const selectedService = await this.selectedServiceChecker(ctx);
    const telegramId = ctx.from?.id;

    const plan = await this.resolvePlanByService(selectedService);

    const clickUrl =
      process.env.BASE_CLICK_URL +
      `?userId=${userId}&planId=${plan._id}&selectedService=${selectedService}`;

    // Use tracking link for Uzcard to capture analytics
    const uzcardUrl = this.createTrackableUzcardLink(telegramId, userId, plan._id.toString(), selectedService);

    return new InlineKeyboard()
      .url('🏦 Uzcard/Humo (30 kun bepul)', uzcardUrl)
      .row()
      .url('💳 Click (30 kun bepul)', clickUrl)
      .row()
      .text('📲 Payme (30 kun bepul)', 'payme_subscription_unavailable')
      .row()
      .text('🔙 Orqaga', 'back_to_payment_types')
      .row()
      .text('🏠 Asosiy menyu', 'main_menu');
  }

  private userHasActiveSubscription(user: IUserDocument): boolean {
    // Check if user has both isActive flag and subscriptionEnd date
    if (!user.isActive || !user.subscriptionEnd) {
      logger.debug('User subscription check failed', {
        telegramId: user.telegramId,
        isActive: user.isActive,
        hasSubscriptionEnd: Boolean(user.subscriptionEnd)
      });
      return false;
    }

    const subscriptionEnd =
      user.subscriptionEnd instanceof Date
        ? user.subscriptionEnd
        : new Date(user.subscriptionEnd);

    const isActive = subscriptionEnd.getTime() > Date.now();

    logger.debug('User subscription time check', {
      telegramId: user.telegramId,
      subscriptionEnd: subscriptionEnd.toISOString(),
      currentTime: new Date().toISOString(),
      isActive
    });

    return isActive;
  }

  private async getLastSuccessfulProvider(
    userId: string,
  ): Promise<string | undefined> {
    const transaction = await Transaction.findOne({
      userId,
      status: TransactionStatus.PAID,
    })
      .sort({ createdAt: -1 })
      .exec();

    return transaction?.provider;
  }

  private getAlreadyPaidMessage(provider?: string): string {
    switch (provider) {
      case 'click':
        return "✅ Kechirasiz, sizda allaqon faol obuna mavjud! Siz Click orqali to'lov qilgansiz. Obuna muddati tugagach qayta urinib ko'ring.";
      case 'payme':
        return "✅ Kechirasiz, sizda allaqon faol obuna mavjud! Siz Payme orqali to'lov qilgansiz. Obuna muddati tugagach qayta urinib ko'ring.";
      case 'uzcard':
        return "✅ Kechirasiz, sizda allaqon faol obuna mavjud! Siz Uzcard orqali to'lov qilgansiz. Obuna muddati tugagach qayta urinib ko'ring.";
      default:
        return "✅ Kechirasiz, sizda allaqon faol obuna mavjud! Obuna muddati tugagach qayta to'lov qilishingiz mumkin.";
    }
  }

  private buildOneTimePaymentCallback(
    provider: 'uzcard' | 'payme' | 'click',
  ): string {
    return `onetime|${provider}`;
  }

  private async handleOneTimePaymentProviderSelection(
    ctx: BotContext,
    provider: 'uzcard' | 'payme' | 'click',
  ): Promise<void> {
    const telegramId = ctx.from?.id;

    if (!telegramId) {
      await this.safeAnswerCallback(ctx, {
        text: "Foydalanuvchi ma'lumotlari topilmadi. Iltimos, qayta urinib ko'ring.",
        show_alert: true,
      });
      return;
    }

    const user = await UserModel.findOne({ telegramId }).exec();

    if (!user) {
      await this.safeAnswerCallback(ctx, {
        text: "Foydalanuvchi ma'lumotlari topilmadi. Iltimos, qayta boshlang.",
        show_alert: true,
      });
      return;
    }

    if (this.userHasActiveSubscription(user)) {
      await this.safeAnswerCallback(ctx, {
        text: 'Sizda allaqachon faol obuna mavjud. Yangi to‘lov talab qilinmaydi.',
        show_alert: true,
      });
      return;
    }

    const selectedService = await this.selectedServiceChecker(ctx);

    let plan: IPlanDocument | null = null;

    if (ctx.session.pendingOnetimePlanId) {
      plan = await Plan.findById(ctx.session.pendingOnetimePlanId).exec();
    }

    if (!plan) {
      plan = await this.resolvePlanByService(selectedService);
      ctx.session.pendingOnetimePlanId = plan._id.toString();
    }

    const userId = user._id.toString();
    let redirectUrl: string | undefined;

    switch (provider) {
      case 'uzcard': {
        const baseUrl = process.env.BASE_UZCARD_ONETIME_URL;
        if (!baseUrl) {
          await this.safeAnswerCallback(ctx, {
            text: "Uzcard to'lov havolasi sozlanmagan. Iltimos, administrator bilan bog'laning.",
            show_alert: true,
          });
          return;
        }
        redirectUrl = `${baseUrl}/?userId=${userId}&planId=${plan._id}&selectedService=${selectedService}`;
        break;
      }
      case 'payme': {
        redirectUrl = generatePaymeLink({
          planId: plan._id.toString(),
          amount: plan.price,
          userId,
        });
        break;
      }
      case 'click': {
        const redirectURLParams: ClickRedirectParams = {
          userId,
          planId: plan._id.toString(),
          amount: plan.price as number,
        };
        redirectUrl = getClickRedirectLink(redirectURLParams);
        break;
      }
      default:
        await this.safeAnswerCallback(ctx, {
          text: "Noma'lum to'lov turi tanlandi.",
          show_alert: true,
        });
        return;
    }

    if (!redirectUrl) {
      await this.safeAnswerCallback(ctx, {
        text: "To'lov havolasi tayyorlanmadi. Iltimos, administrator bilan bog'laning.",
        show_alert: true,
      });
      return;
    }

    const providerTitles: Record<'uzcard' | 'payme' | 'click', string> = {
      uzcard: "📲 Uzcard orqali to'lash",
      payme: "📲 Payme orqali to'lash",
      click: "💳 Click orqali to'lash",
    };

    const keyboard = new InlineKeyboard()
      .url(providerTitles[provider], redirectUrl)
      .row()
      .text("🔄 To'lov turlariga qaytish", 'payment_type_onetime')
      .row()
      .text('🏠 Asosiy menyu', 'main_menu');

    try {
      await ctx.editMessageText(
        `${providerTitles[provider]} ni tanladingiz.\n\nQuyidagi tugma orqali to'lovni amalga oshirishingiz mumkin:`,
        {
          reply_markup: keyboard,
          disable_web_page_preview: true,
        },
      );
    } catch (error) {
      logger.warn('Failed to edit onetime payment message, sending new one', {
        error,
      });
      await ctx.reply(
        `${providerTitles[provider]} ni tanladingiz.\n\nQuyidagi tugma orqali to'lovni amalga oshirishingiz mumkin:`,
        {
          reply_markup: keyboard,
          disable_web_page_preview: true,
        },
      );
    }

    await this.safeAnswerCallback(ctx);
  }

  private async revokeUserInviteLink(
    user?: IUserDocument | null,
    save = true,
  ): Promise<void> {
    if (!user?.activeInviteLink) {
      return;
    }

    logger.info('Clearing stored invite link without revoking to keep it active', {
      telegramId: user.telegramId,
    });

    user.activeInviteLink = undefined;

    if (save) {
      try {
        await user.save();
      } catch (error) {
        logger.warn('Failed to save user after revoking invite link', {
          telegramId: user.telegramId,
          error,
        });
      }
    }
  }

  private async handleChatJoinRequest(ctx: BotContext): Promise<void> {
    const joinRequest = ctx.chatJoinRequest;
    if (!joinRequest) {
      return;
    }

    const telegramId = joinRequest.from.id;
    const chatId = joinRequest.chat.id;

    try {
      const user = await UserModel.findOne({ telegramId }).exec();

      if (user && this.userHasActiveSubscription(user)) {
        await ctx.api.approveChatJoinRequest(chatId, telegramId);
        logger.info('Join request approved', { telegramId, chatId });
        return;
      }

      await ctx.api.declineChatJoinRequest(chatId, telegramId);
      logger.info('Join request declined due to inactive subscription', {
        telegramId,
        chatId,
      });

      try {
        await ctx.api.sendMessage(
          telegramId,
          "❌ Obunangiz faol emas. Kanalga kirish uchun iltimos, obunani yangilang.",
        );
      } catch (error) {
        logger.warn('Failed to notify user about declined join request', {
          telegramId,
          error,
        });
      }
    } catch (error) {
      logger.error('Error processing join request', {
        telegramId,
        chatId,
        error,
      });
    }
  }

  private async handleAdminCommand(ctx: BotContext): Promise<void> {
    logger.info(`Admin command issued by user ID: ${ctx.from?.id}`);

    if (!this.ADMIN_IDS.includes(ctx.from?.id || 0)) {
      logger.info(`Authorization failed for ID: ${ctx.from?.id}`);
      return;
    }

    const totalUsers = await UserModel.countDocuments();
    const activeUsers = await UserModel.countDocuments({ isActive: true });

    const today = new Date();
    today.setHours(0, 0, 0, 0);

    const todayTimestamp = Math.floor(today.getTime() / 1000);

    const newUsersToday = await UserModel.countDocuments({
      _id: {
        $gt: new mongoose.Types.ObjectId(todayTimestamp),
      },
    });

    const newSubscribersToday = await UserModel.countDocuments({
      subscriptionStart: { $gte: today },
      isActive: true,
    });

    const expiredSubscriptions = await UserModel.countDocuments({
      isActive: false,
      subscriptionEnd: { $exists: true, $ne: null },
    });

    const threeDaysFromNow = new Date();
    threeDaysFromNow.setDate(threeDaysFromNow.getDate() + 3);

    const expiringIn3Days = await UserModel.countDocuments({
      subscriptionEnd: {
        $gte: new Date(),
        $lte: threeDaysFromNow,
      },
      isActive: true,
    });

    const neverSubscribed = await UserModel.countDocuments({
      $or: [
        { subscriptionStart: { $exists: false } },
        { subscriptionStart: null },
      ],
    });

    //Autosubscription qilinmadi keyin qilaman
    const totalCardStats = await UserCardsModel.aggregate([
      { $match: { verified: true } },
      {
        $group: {
          _id: '$cardType',
          count: { $sum: 1 },
        },
      },
    ]);

    const totalCards = totalCardStats.reduce((acc, cur) => acc + cur.count, 0);
    const totalCardBreakdown: Record<string, number> = {
      click: 0,
      uzcard: 0,
      payme: 0,
    };
    totalCardStats.forEach((stat) => {
      totalCardBreakdown[stat._id] = stat.count;
    });

    // Cards added today
    const todayCardStats = await UserCardsModel.aggregate([
      {
        $match: {
          verified: true,
          createdAt: { $gte: today },
        },
      },
      {
        $group: {
          _id: '$cardType',
          count: { $sum: 1 },
        },
      },
    ]);

    const todayCardTotal = todayCardStats.reduce(
      (acc, cur) => acc + cur.count,
      0,
    );
    const todayCardBreakdown: Record<string, number> = {
      click: 0,
      uzcard: 0,
      payme: 0,
    };
    todayCardStats.forEach((stat) => {
      todayCardBreakdown[stat._id] = stat.count;
    });

    const startOfDay = new Date();
    startOfDay.setHours(0, 0, 0, 0);

    const completedSubscription = await UserCardsModel.countDocuments({
      verified: true,
      createdAt: { $gte: startOfDay },
    });

    //

    const since24 = new Date(Date.now() - 24 * 60 * 60 * 1000);

    const [
      totalStarts,
      totalFreeTrialClicks,
      totalTermsOpens,
      totalUzcardOpens,
      startsLast24,
      freeTrialLast24,
      termsLast24,
      uzcardLast24,
    ] = await Promise.all([
      BotInteractionStatsModel.countDocuments({ started: true }),
      BotInteractionStatsModel.countDocuments({ clickedFreeTrial: true }),
      BotInteractionStatsModel.countDocuments({ openedTerms: true }),
      BotInteractionStatsModel.countDocuments({ openedUzcard: true }),

      // Last 24 hours (use updatedAt to capture when the flag was set)
      BotInteractionStatsModel.countDocuments({ started: true, updatedAt: { $gte: since24 } }),
      BotInteractionStatsModel.countDocuments({ clickedFreeTrial: true, updatedAt: { $gte: since24 } }),
      BotInteractionStatsModel.countDocuments({ openedTerms: true, updatedAt: { $gte: since24 } }),
      BotInteractionStatsModel.countDocuments({ openedUzcard: true, updatedAt: { $gte: since24 } }),
    ]);

    const statsMessage = `📊 <b>Bot statistikasi</b>: \n\n` +
      `👥 Umumiy foydalanuvchilar: ${totalUsers} \n` +
      `✅ Umumiy aktiv foydalanuvchilar: ${activeUsers} \n` +
      `🆕 Bugun botga start berganlar: ${newUsersToday} \n` +
      `💸 Bugun kanalga qo'shilgan foydalanuqlar: ${newSubscribersToday} \n` +
      `📉 Obunasi tugaganlar: ${expiredSubscriptions} \n` +
      `⏳ Obunasi 3 kun ichida tugaydiganlar: ${expiringIn3Days} \n` +
      `🚫 Hech qachon obuna bo'lmaganlar: ${neverSubscribed} \n\n` +

      `🧭 <b>Funnel kuzatuvi (jami)</b>: \n\n` +
      `▶️ /start bosgan foydalanuvchilar: ${totalStarts} \n` +
      `🎁 Bepul sinov tugmasini bosganlar: ${totalFreeTrialClicks} \n` +
      `📄 Foydalanish shartlarini ochganlar: ${totalTermsOpens} \n` +
      `💳 Uzcard/Humo havolasini ochganlar: ${totalUzcardOpens} \n\n` +

      `📈 <b>Funnel (oxirgi 24 soat)</b>: \n\n` +
      `▶️ /start (24 soat): ${startsLast24} \n` +
      `🎁 Bepul sinov (24 soat): ${freeTrialLast24} \n` +
      `📄 Shartlar ochish (24 soat): ${termsLast24} \n` +
      `💳 Uzcard/Humo ochish (24 soat): ${uzcardLast24} \n\n` +

      `📊 <b>Avtomatik to'lov statistikasi (bugun)</b>: \n\n` +
      `✅ Karta qo'shganlar: ${completedSubscription} \n\n` +

      `💳 <b>Qo'shilgan kartalar statistikasi</b>: \n\n` +
      `📦 Umumiy qo'shilgan kartalar: ${totalCards} \n` +
      ` 🔵 Uzcard: ${totalCardBreakdown.uzcard} \n` +
      ` 🟡 Click: ${totalCardBreakdown.click} \n` +
      ` 🟣 Payme: ${totalCardBreakdown.payme} \n\n` +
      `📅 <u>Bugun qo'shilgan kartalar</u>: ${todayCardTotal} \n` +
      ` 🔵 Uzcard: ${todayCardBreakdown.uzcard} \n` +
      ` 🟡 Click: ${todayCardBreakdown.click} \n` +
      ` 🟣 Payme: ${todayCardBreakdown.payme} \n\n\n`;

    try {
      // await ctx.reply('Admin command executed successfully.');
      await ctx.reply(statsMessage, {
        parse_mode: "HTML"
      })
    } catch (error) {
      logger.error('Error handling admin command:', error);
      await ctx.reply(
        '❌ Error processing admin command. Please try again later.',
      );
    }
  }


  private async handleDevTestSubscribe(ctx: BotContext): Promise<void> {
    try {
      const telegramId = ctx.from?.id;
      const user = await UserModel.findOne({ telegramId });
      if (!user) {
        await this.safeAnswerCallback(ctx, {
          text: "Foydalanuvchi ID'sini olishda xatolik yuz berdi.",
          show_alert: true,
        });
        return;
      }

      const plan = await Plan.findOne({
        name: 'Basic',
      });

      if (!plan) {
        logger.error('No plan found with name "Basic"');
        return;
      }

      try {
        const { user: subscription, wasKickedOut } =
          await this.subscriptionService.createSubscription(
            user._id as string,
            plan,
            ctx.from?.username,
          );

        if (wasKickedOut && telegramId) {
          await this.bot.api.unbanChatMember(config.CHANNEL_ID, telegramId);
        }

        await this.revokeUserInviteLink(subscription, false);

        const inviteLink = await this.getPrivateLink();
        subscription.activeInviteLink = inviteLink;
        await subscription.save();
        const keyboard = new InlineKeyboard().url('🔗 Kanalga kirish', inviteLink);

        let messageText =
          `🎉 DEV TEST: Muvaffaqiyatli obuna bo'ldingiz!\n\n` +
          `⏰ Obuna tugash muddati: ${subscription.subscriptionEnd.toLocaleDateString()}\n\n` +
          `[DEV MODE] To'lov talab qilinmadi\n\n`;

        if (wasKickedOut) {
          messageText += `ℹ️ Sizning avvalgi bloklanishingiz bekor qilindi. `;
        }

        messageText += `Quyidagi havola orqali kanalga kirishingiz mumkin:`;

        await ctx.editMessageText(messageText, {
          reply_markup: keyboard,
          parse_mode: 'HTML',
        });
      } catch (error) {
        if (
          error instanceof Error &&
          error.message === 'User already has an active subscription'
        ) {
          const keyboard = new InlineKeyboard()
            .text('📊 Obuna holati', 'check_status')
            .row()
            .text('🔙 Asosiy menyu', 'main_menu');

          await ctx.editMessageText(
            '⚠️ Siz allaqachon faol obunaga egasiz. Obuna holatini tekshirish uchun quyidagi tugmani bosing:',
            { reply_markup: keyboard },
          );
          return;
        }
        logger.error('Dev test subscription error:', error);
        await this.safeAnswerCallback(ctx, {
          text: 'Obunani tasdiqlashda xatolik yuz berdi.',
          show_alert: true,
        });
      }
    } catch (error) {
      logger.error('Dev test subscription error:', error);
      await this.safeAnswerCallback(ctx, {
        text: 'Dev test obunasini yaratishda xatolik yuz berdi.',
        show_alert: true,
      });
    }
  }

  private async selectedServiceChecker(ctx: BotContext): Promise<string> {
    let selectedService = ctx.session.selectedService;

    if (!selectedService) {
      selectedService = 'yulduz';
      ctx.session.selectedService = selectedService;
    }

    await this.resolvePlanByService(selectedService);

    return selectedService;
  }

  private async safeAnswerCallback(
    ctx: BotContext,
    payload?: Parameters<BotContext['answerCallbackQuery']>[0],
  ): Promise<void> {
    try {
      if (payload) {
        await ctx.answerCallbackQuery(payload as any);
      } else {
        await ctx.answerCallbackQuery();
      }
    } catch (error) {
      if (error instanceof GrammyError && error.error_code === 400) {
        if (typeof (logger as any).debug === 'function') {
          (logger as any).debug('Stale callback ignored', {
            telegramId: ctx.from?.id,
            description: error.description,
          });
        }
      } else {
        logger.warn('answerCallbackQuery failed', {
          telegramId: ctx.from?.id,
          error,
        });
      }
    }
  }

  // Helper methods for creating tracking links
  private createTrackableTermsLink(telegramId: number): string {
    const token = createTrackingToken(telegramId, 'terms');
    const baseUrl = process.env.BASE_URL || 'http://213.230.110.176:8989';
    return `${baseUrl}/api/bot/redirect/terms?token=${token}`;
  }

  private createTrackableUzcardLink(telegramId: number, userId: string, planId: string, selectedService: string): string {
    const token = createTrackingToken(telegramId, 'uzcard');
    const baseUrl = process.env.BASE_URL || 'http://213.230.110.176:8989';
    const redirectUrl = `${baseUrl}/api/bot/redirect/uzcard?token=${token}`;

    // We'll modify the controller to handle this redirect properly
    return `${redirectUrl}&userId=${userId}&planId=${planId}&selectedService=${selectedService}`;
  }

}
